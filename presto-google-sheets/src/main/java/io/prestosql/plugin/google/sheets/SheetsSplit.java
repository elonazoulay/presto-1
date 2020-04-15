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
package io.prestosql.plugin.google.sheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SheetsSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final HostAddress address;
    private final SheetDataLocation sheetDataLocation;

    @JsonCreator
    public SheetsSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("sheetDataLocation") SheetDataLocation sheetDataLocation,
            @JsonProperty("address") HostAddress address)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.sheetDataLocation = requireNonNull(sheetDataLocation, "sheetExpression is null");
        this.address = address;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public SheetDataLocation getSheetDataLocation()
    {
        return sheetDataLocation;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("schemaName", schemaName)
                .put("tableName", tableName)
                .build();
    }
}
