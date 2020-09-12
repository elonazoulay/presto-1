/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.pinot.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.pinot.table.EncoderFactory.createEncoder;
import static java.util.Objects.requireNonNull;

public class GenericRowBuffer
{
    private final List<FieldSpec> fieldSpecs;
    private final TimeFieldSpec timeFieldSpec;
    private final List<Encoder> encoders;
    private final Function<GenericRow, String> rowToSegmentFunction;
    private final Map<String, List<GenericRow>> segmentRows = new HashMap<>();
    private final Optional<Comparator<GenericRow>> rowComparator;

    public GenericRowBuffer(List<FieldSpec> fieldSpecs, TimeFieldSpec timeFieldSpec, List<Type> prestoTypes, Function<GenericRow, String> rowToSegmentFunction, Optional<Comparator<GenericRow>> rowComparator)
    {
        requireNonNull(fieldSpecs, "pinotFields is null");
        requireNonNull(prestoTypes, "prestoTypes is null");
        checkState(fieldSpecs.size() == prestoTypes.size(), "FieldSpecs do not match prestoTypes");
        checkState(!fieldSpecs.isEmpty(), "fieldSpecs is empty");
        this.fieldSpecs = ImmutableList.copyOf(fieldSpecs);
        this.timeFieldSpec = requireNonNull(timeFieldSpec, "timeFieldSpec is null");
        ImmutableList.Builder<Encoder> encodersBuilder = ImmutableList.builder();
        for (int i = 0; i < fieldSpecs.size(); i++) {
            encodersBuilder.add(createEncoder(fieldSpecs.get(i), prestoTypes.get(i)));
        }
        encoders = encodersBuilder.build();
        this.rowToSegmentFunction = requireNonNull(rowToSegmentFunction, "rowToSegmentFunction is null");
        this.rowComparator = requireNonNull(rowComparator, "rowComparator is null");
    }

    public void append(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(page.getChannelCount() == encoders.size(), "Unexpected channel count for page: %s", page.getChannelCount());
        GenericRow[] rows = new GenericRow[page.getBlock(0).getPositionCount()];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new GenericRow();
        }
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            String fieldName = fieldSpecs.get(channel).getName();
            if (fieldName.equals(timeFieldSpec.getOutgoingGranularitySpec().getName())) {
                fieldName = timeFieldSpec.getIncomingGranularitySpec().getName();
            }
            Encoder encoder = encoders.get(channel);
            for (int position = 0; position < block.getPositionCount(); position++) {
                rows[position].putValue(fieldName, encoder.encode(block, position));
            }
        }
        for (GenericRow row : rows) {
            String segment = rowToSegmentFunction.apply(row);
            segmentRows.computeIfAbsent(segment, k -> new ArrayList<>()).add(row);
        }
    }

    public Map<String, GenericRowRecordReader> build()
    {
        Map<String, GenericRowRecordReader> recordReaderBuilder = new HashMap<>(segmentRows.size());
        if (rowComparator.isPresent()) {
            for (List<GenericRow> rows : segmentRows.values()) {
                Collections.sort(rows, rowComparator.get());
            }
        }
        for (Map.Entry<String, List<GenericRow>> entry : segmentRows.entrySet()) {
            recordReaderBuilder.put(entry.getKey(), new GenericRowRecordReader(entry.getValue()));
        }
        segmentRows.clear();
        return recordReaderBuilder;
    }
}
