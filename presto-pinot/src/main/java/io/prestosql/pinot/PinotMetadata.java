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
package io.prestosql.pinot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.pinot.client.PinotClient;
import io.prestosql.pinot.query.AggregationExpression;
import io.prestosql.pinot.query.DynamicTable;
import io.prestosql.pinot.query.DynamicTableBuilder;
import io.prestosql.pinot.table.PinotTimeFieldSpec;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTablePartitioning;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.pinot.PinotColumn.getPinotColumnsForPinotSchema;
import static io.prestosql.pinot.client.PinotClient.getFromCache;
import static io.prestosql.pinot.query.DynamicTableBuilder.BIGINT_AGGREGATIONS;
import static io.prestosql.pinot.query.DynamicTableBuilder.DOUBLE_AGGREGATIONS;
import static io.prestosql.pinot.table.PrestoToPinotSchemaConverter.convert;
import static io.prestosql.pinot.table.PrestoToPinotSchemaConverter.getOfflineConfig;
import static io.prestosql.pinot.table.PrestoToPinotSchemaConverter.getRealtimeConfig;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PinotMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    private static final String PINOT_COLUMN_NAME_PROPERTY = "pinotColumnName";

    private final PinotClient pinotClient;
    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final String segmentCreationBaseDirectory;
    private final NodeManager nodeManager;

    @Inject
    public PinotMetadata(
            PinotClient pinotClient,
            PinotConfig pinotConfig,
            @ForPinot Executor executor,
            NodeManager nodeManager)
    {
        requireNonNull(pinotConfig, "pinot config");
        this.segmentCreationBaseDirectory = pinotConfig.getSegmentCreationBaseDirectory();
        long metadataCacheExpiryMillis = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClient.getTableSchema(tableName);
                                return getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        }, executor));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Schema schema = convert(tableMetadata);
        Optional<TableConfig> realtimeConfig = getRealtimeConfig(schema, tableMetadata);
        TableConfig offlineConfig = getOfflineConfig(schema, tableMetadata);
        pinotClient.createSchema(schema);
        realtimeConfig.ifPresent(pinotClient::createTable);
        pinotClient.createTable(offlineConfig);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.getTableName().trim().startsWith("select ")) {
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(this, pinotClient, tableName);
            return new PinotTableHandle(tableName.getSchemaName(), dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable), Optional.empty(), Optional.empty());
        }
        String pinotTableName = pinotClient.getPinotTableNameFromPrestoTableNameIfExists(tableName.getTableName());
        if (pinotTableName == null) {
            return null;
        }
        return new PinotTableHandle(
                tableName.getSchemaName(),
                pinotTableName,
                Optional.of(getNodes()),
                getTimeFieldSpec(pinotTableName));
    }

    private List<String> getNodes()
    {
        List<String> nodes = nodeManager.getRequiredWorkerNodes().stream().map(Node::getNodeIdentifier).collect(toList());
        Collections.shuffle(nodes);
        return nodes;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.getQuery().isPresent()) {
            DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            for (String columnName : dynamicTable.getSelections()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (String columnName : dynamicTable.getGroupingColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            SchemaTableName schemaTableName = new SchemaTableName(pinotTableHandle.getSchemaName(), dynamicTable.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : pinotClient.getPinotTableNames()) {
            builder.add(new SchemaTableName(SCHEMA_NAME, table));
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        if (pinotTableHandle.getQuery().isPresent()) {
            return getDynamicTableColumnHandles(pinotTableHandle);
        }
        return getPinotColumnHandles(pinotTableHandle.getTableName());
    }

    public Map<String, ColumnHandle> getPinotColumnHandles(String tableName)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getColumnsMetadata(tableName)) {
            columnHandlesBuilder.put(columnMetadata.getName(),
                    new PinotColumnHandle(getPinotColumnName(columnMetadata), columnMetadata.getType()));
        }
        return columnHandlesBuilder.build();
    }

    private static String getPinotColumnName(ColumnMetadata columnMetadata)
    {
        Object pinotColumnName = requireNonNull(columnMetadata.getProperties().get(PINOT_COLUMN_NAME_PROPERTY), "Pinot column name is missing");
        return pinotColumnName.toString();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((PinotColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        try {
            PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
            if (pinotTableHandle.getTimeFieldSpec().isPresent()) {
                FieldSpec timeFieldSpec = pinotTableHandle.getTimeFieldSpec().get().toTimeFieldSpec();
                Type type = PinotColumn.getPrestoTypeFromPinotType(timeFieldSpec.getDataType());
                PinotColumnHandle partitionColumn = new PinotColumnHandle(timeFieldSpec.getName().toLowerCase(ENGLISH), type);
                return new ConnectorTableProperties(
                        TupleDomain.all(),
                        Optional.of(new ConnectorTablePartitioning(new PinotPartitioningHandle(pinotTableHandle.getNodes(), pinotTableHandle.getTimeFieldSpec()), ImmutableList.of(partitionColumn))),
                        Optional.of(ImmutableSet.of(partitionColumn)),
                        Optional.empty(),
                        ImmutableList.of());
            }
            return new ConnectorTableProperties();
        }
        catch (Exception e) {
            // TODO: rethrow runtime exception, throw pinot exception
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        return new PinotTableHandle(
                pinotTableHandle.getSchemaName(),
                pinotTableHandle.getTableName(),
                pinotTableHandle.getConstraint(),
                pinotTableHandle.getLimit(),
                pinotTableHandle.getQuery(),
                pinotPartitioningHandle.getNodes(),
                pinotPartitioningHandle.getTimeFieldSpec());
    }

    private Optional<PinotTimeFieldSpec> getTimeFieldSpec(String pinotTableName)
    {
        Schema schema = pinotClient.getTableSchema(pinotTableName);
        if (schema.getTimeFieldSpec() == null) {
            return Optional.empty();
        }
        return Optional.of(new PinotTimeFieldSpec(schema.getTimeFieldSpec()));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        Optional<DynamicTable> dynamicTable = handle.getQuery();
        if (dynamicTable.isPresent() &&
                (!dynamicTable.get().getLimit().isPresent() || dynamicTable.get().getLimit().getAsLong() > limit)) {
            dynamicTable = Optional.of(new DynamicTable(dynamicTable.get().getTableName(),
                    dynamicTable.get().getSuffix(),
                    dynamicTable.get().getSelections(),
                    dynamicTable.get().getGroupingColumns(),
                    dynamicTable.get().getFilter(),
                    dynamicTable.get().getAggregateColumns(),
                    dynamicTable.get().getOrderBy(),
                    OptionalLong.of(limit),
                    dynamicTable.get().getOffset(),
                    dynamicTable.get().getQuery()));
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit),
                dynamicTable,
                handle.getNodes(),
                handle.getTimeFieldSpec());
        return Optional.of(new LimitApplicationResult<>(handle, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit(),
                handle.getQuery(),
                handle.getNodes(),
                handle.getTimeFieldSpec());
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        String pinotTableName = pinotClient.getPinotTableNameFromPrestoTableName(((PinotTableHandle) tableHandle).getTableName());
        List<PinotColumnHandle> pinotColumnHandles = columns.stream()
                .map(column -> (PinotColumnHandle) column)
                .collect(toImmutableList());
        return new PinotInsertTableHandle(pinotTableName, pinotColumnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @VisibleForTesting
    public List<PinotColumn> getPinotColumns(String tableName)
    {
        String pinotTableName = pinotClient.getPinotTableNameFromPrestoTableName(tableName);
        return getFromCache(pinotTableColumnCache, pinotTableName);
    }

    private Map<String, ColumnHandle> getDynamicTableColumnHandles(PinotTableHandle pinotTableHandle)
    {
        checkState(pinotTableHandle.getQuery().isPresent(), "dynamic table not present");
        String schemaName = pinotTableHandle.getSchemaName();
        DynamicTable dynamicTable = pinotTableHandle.getQuery().get();

        Map<String, ColumnHandle> columnHandles = getPinotColumnHandles(dynamicTable.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (String columnName : dynamicTable.getSelections()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (String columnName : dynamicTable.getGroupingColumns()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
            if (DOUBLE_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), DOUBLE));
            }
            else if (BIGINT_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), BIGINT));
            }
            else {
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getBaseColumnName().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), aggregationExpression.getBaseColumnName());
                }
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(),
                                columnHandle.getDataType()));
            }
        }
        return columnHandlesBuilder.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(tableName, getColumnsMetadata(tableName.getTableName()));
    }

    private List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        List<PinotColumn> columns = getPinotColumns(tableName);
        return columns.stream()
                .map(PinotMetadata::createPinotColumnMetadata)
                .collect(toImmutableList());
    }

    private static ColumnMetadata createPinotColumnMetadata(PinotColumn pinotColumn)
    {
        return ColumnMetadata.builder()
                .setName(pinotColumn.getName().toLowerCase(ENGLISH))
                .setType(pinotColumn.getType())
                .setProperties(ImmutableMap.<String, Object>builder()
                        .put(PINOT_COLUMN_NAME_PROPERTY, pinotColumn.getName())
                        .build())
                .build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (!prefix.getSchema().isPresent() || !prefix.getTable().isPresent()) {
            return listTables(session, Optional.empty());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }
}
