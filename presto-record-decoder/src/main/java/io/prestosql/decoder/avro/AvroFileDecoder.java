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
package io.prestosql.decoder.avro;

import io.prestosql.spi.PrestoException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroFileDecoder<T>
        implements AvroDataDecoder<T>
{
    public static final String NAME = "file";

    private final AvroReaderSupplier<T> avroReaderSupplier;

    public AvroFileDecoder(AvroReaderSupplier<T> avroReaderSupplier)
    {
        this.avroReaderSupplier = requireNonNull(avroReaderSupplier, "datumReaderSupplier is null");
    }

    @Override
    public T read(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        DatumReader<T> avroReader = avroReaderSupplier.get(buffer);
        try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(data, buffer.position(), data.length - buffer.position()), avroReader)) {
            if (!dataFileReader.hasNext()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "No avro record found");
            }
            T avroValue = dataFileReader.next();
            if (dataFileReader.hasNext()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected extra record found");
            }
            return avroValue;
        }
        catch (AvroRuntimeException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
    }

    public static class Factory
            implements AvroDataDecoderFactory
    {
        @Override
        public <T> AvroDataDecoder<T> create(Map<String, String> decoderParams, AvroReaderSupplier<T> avroReaderSupplier)
        {
            return new AvroFileDecoder<>(avroReaderSupplier);
        }
    }
}
