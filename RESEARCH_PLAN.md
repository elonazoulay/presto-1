# Trino Streaming Connectors — Research Plan

## Goal
Compare two architectures for getting data from ClickHouse (and Pinot) through Trino with minimal coordinator memory overhead.

## Architecture A: Thin Proxy (Coordinator-Only)

```
Client → Trino Coordinator → ClickHouse HTTP streaming
                                 (no workers, no buffering)
```

- Coordinator connects directly to ClickHouse via HTTP
- Uses `TabSeparatedWithNamesAndTypes` or `Native` format
- Implements `ConnectorPageSource` that yields pages as rows stream in
- **No JDBC** — raw HTTP with chunked transfer encoding
- ClickHouse sends rows, Trino forwards them as pages immediately
- Backpressure: HTTP chunked encoding handles flow control naturally

### Key Implementation
- New module: `plugin/trino-clickhouse-streaming/`
- `ClickHouseStreamingClient` — HTTP client using ClickHouse HTTP API
- `ClickHouseStreamingPageSource` — implements `ConnectorPageSource`, converts stream → pages
- `ClickHouseStreamingSplit` — represents one query/split
- Config: `clickhouse-streaming.properties`

### Pros
- Simplest architecture
- Lowest latency to first row
- Minimal coordinator memory (stream through)
- ClickHouse does all the compute

### Cons
- No parallelism (single coordinator thread per query)
- ClickHouse must handle all computation (no Trino pushdown beyond WHERE)
- Single point of failure

## Architecture B: Worker Mode (Table Function)

```
Client → Trino Coordinator → Workers → ClickHouse (parallel reads)
                                        (each worker reads a partition)
```

- Modeled after PostgreSQL FDW / table function pattern
- ClickHouse query is treated as a **table function** in Trino
- Coordinator splits the query into partitions (by `_partition` or ranges)
- Each worker opens its own HTTP connection to ClickHouse for its partition
- Workers process and combine results

### Key Implementation
- New module: `plugin/trino-clickhouse-table-function/`
- `ClickHouseTableFunction` — registers as a Trino table function
- `ClickHouseSplitManager` — creates splits (one per partition/shard)
- `ClickHouseWorkerPageSource` — each worker streams its partition
- Config: `clickhouse-tf.properties`

### ClickHouse Partition Splitting
```sql
-- Get partitions
SELECT DISTINCT partition FROM system.parts WHERE table = 'xxx'

-- Each worker queries one partition
SELECT * FROM table WHERE _partition = '202401' FORMAT TabSeparatedWithNamesAndTypes
```

### Pros
- Parallel reads = higher throughput
- Workers share memory load
- Can handle larger datasets
- Trino can do additional computation on results

### Cons
- More complex setup (needs workers)
- Requires ClickHouse table to have partitions
- More moving parts

## Benchmark Plan

### Dataset
- ClickHouse Docker container with `ontime` dataset (flight data, ~200M rows)
- Or generate synthetic: `CREATE TABLE test ... ENGINE = MergeTree ORDER BY id`

### Metrics
| Metric | Architecture A | Architecture B |
|--------|---------------|---------------|
| Peak coordinator memory | ? | ? |
| Peak total memory | ? | ? |
| Rows/sec throughput | ? | ? |
| Latency to first row | ? | ? |
| 10M row scan time | ? | ? |
| 100M row scan time | ? | ? |

### Queries
1. `SELECT COUNT(*) FROM large_table` (aggregation)
2. `SELECT * FROM large_table LIMIT 1000000` (large scan)
3. `SELECT col, COUNT(*) FROM large_table GROUP BY col` (group by)

## Setup

### Docker Compose
```yaml
services:
  trino:
    image: trinodb/trino:latest
    ports: ["8080:8080"]
    volumes: [./etc:/etc/trino]

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports: ["8123:8123", "9000:9000"]

  pinot:
    image: apachepinot/pinot:latest
    command: QuickStart
    ports: ["9000:9000"]
```

## Execution Order

1. ✅ Create branch `research/streaming-connectors`
2. Docker Compose with Trino + ClickHouse
3. Build Architecture A (thin proxy) — start here, simpler
4. Benchmark Architecture A
5. Build Architecture B (worker mode)
6. Benchmark Architecture B
7. Compare and document findings

## Pinot Notes
- Existing Pinot connector already uses grpc streaming
- Evaluate if it's "thin enough" or needs the same treatment
- Pinot's query broker already handles partitioning — may not need Architecture B
