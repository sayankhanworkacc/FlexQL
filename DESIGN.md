# FlexQL — Design Document

## Repository

> https://github.com/sayankhanworkacc/FlexQL

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Storage](#2-data-storage)
3. [Indexing](#3-indexing)
4. [Caching Strategy](#4-caching-strategy)
5. [Expiration Timestamps](#5-expiration-timestamps)
6. [Multithreading Design](#6-multithreading-design)
7. [Query Engine](#7-query-engine)
8. [Wire Protocol](#8-wire-protocol)
9. [Additional Design Decisions](#9-additional-design-decisions)
10. [Performance Results](#10-performance-results)

---

## 1. System Overview

FlexQL is a client-server in-memory relational database written entirely in C++17, with no external database libraries. The server maintains all data in RAM. The client library (`flexql.cpp`) communicates with the server over a TCP socket using a simple text protocol.

```
┌──────────────────────────────────────────────────┐
│                  Client Process                  │
│                                                  │
│  flexql_open()  ──TCP──►  FlexQL Server          │
│  flexql_exec()  ──SQL;──► Parser → Executor      │
│  callback()     ◄─ROW──   Storage + Index        │
│  flexql_close() ────────►                        │
└──────────────────────────────────────────────────┘
```

The entire server-side implementation lives in `flexql_server.cpp` (single file, ~1100 lines) for simplicity of compilation and submission.

---

## 2. Data Storage

### Row-Major In-Memory Store

Each table is stored as a `std::vector<Row>`, where every `Row` holds a `std::vector<std::string>` of field values. All values are stored as strings regardless of declared type; type information is used only during comparisons (numeric vs. lexicographic).

**Why row-major?**

The supported SQL subset (point lookups, single-condition WHERE, INNER JOIN, row callbacks) is inherently row-oriented. Every result row must be assembled as a unit and delivered to the callback. A column store would require reassembling scattered per-column arrays on every row delivery, adding overhead without benefit for this query set.

**Why strings for all values?**

Avoiding a tagged union or `std::variant` simplifies the parser, executor, and comparison code. Numeric comparisons are done at evaluation time via `strtod()`, which is accurate for the value ranges used in practice (INT and DECIMAL columns). This also makes NULL representation trivial.

### Schema Storage

Each table owns a `Schema` struct containing the table name and a `std::vector<ColDef>`, each holding a column name and `ColType` enum (`DECIMAL`, `VARCHAR`, `TEXT`, `DATETIME`). The schema is stored with the table and consulted for column name resolution.

### Memory Layout

```
Catalog
  └── unordered_map<string, shared_ptr<Table>>
        Table
          ├── Schema (col names + types)
          ├── vector<Row>  ← contiguous row storage
          ├── BTree<string, uint64_t>  ← PK index
          └── shared_mutex  ← per-table concurrency
```

---

## 3. Indexing

### B+ Tree on Primary Key (First Column)

A B+ tree with **fan-out (Order) = 128** is maintained per table on the first declared column (treated as the primary key). Internal nodes hold up to 127 keys and 128 child pointers. Leaf nodes hold up to 127 key/value pairs (PK string → row index in vector) and are linked via right-sibling pointers.

**Why B+ Tree over hash map?**

The assignment requires WHERE clauses with `=`, `>`, `<`, `>=`, `<=`. A hash map handles only `=` efficiently; all range operators degrade to O(n) scan. The B+ tree handles all five operators natively:

| Operator | B+ Tree approach |
|----------|-----------------|
| `=`      | Point lookup — `O(log n)` |
| `>`      | Scan forward from matching leaf — `O(log n + k)` |
| `>=`     | Scan forward from matching leaf (inclusive) — `O(log n + k)` |
| `<`      | Scan from leftmost leaf until key ≥ value — `O(k)` |
| `<=`     | Scan from leftmost leaf until key > value — `O(k)` |

With fan-out 128, a table of 10 million rows has tree depth ≤ 4 (128⁴ = 268M), meaning any point lookup costs at most 4 node traversals.

**Index coverage:** Only the primary key (first column) is indexed. Non-PK WHERE clauses (e.g., `WHERE BALANCE > 1000`) perform a sequential scan of the row vector, which is cache-friendly and efficient for in-memory access.

---

## 4. Caching Strategy

### LRU (Least-Recently-Used) Query Result Cache

The executor maintains an LRU cache mapping **SQL text → (column_names, result_rows)** with capacity **8,192 entries**.

**Implementation:** Doubly-linked list + `unordered_map<string, iterator>`. Cache hit = O(1) promotion to MRU position. Cache miss = O(1) eviction of LRU entry on overflow.

**Invalidation:** Whenever an INSERT touches table `T`, all cache entries whose SQL text contains the substring `T` are evicted. This is conservative (may evict more than necessary) but always correct — reads never see stale data after a write.

**TTL bypass:** Tables that contain any rows with an expiration timestamp skip the cache entirely for queries. This is necessary because the same SQL text could return different results as rows expire with time.

**Why LRU over ARC or LFU?**

ARC requires two ghost lists and a dynamic balance parameter, adding implementation complexity and a larger correctness surface. For the benchmark's access pattern (bulk insert → repeated queries), plain LRU captures all the benefit: after the first SELECT, repeated identical queries are served from cache in O(1). LRU's worst case (one-time large scan evicting hot entries) is naturally avoided here because INSERTs invalidate the cache before any SELECT scan runs.

---

## 5. Expiration Timestamps

### Dual-mode expiration

**Column-based (used by benchmark):** The benchmark stores expiration as a regular `DECIMAL` column named `EXPIRES_AT` with a Unix epoch value (e.g., `1893456000` ≈ year 2030). The server treats this as ordinary data — no special enforcement. This matches how the assignment spec said "details described later" without specifying enforcement.

**TTL keyword (optional extension):** A `TTL <seconds>` suffix on INSERT sets a hidden `expires_at_ns` field in the row to `now + seconds` (nanoseconds since epoch). This is invisible to the client and enforced lazily at read time:

```sql
INSERT INTO cache_table VALUES (1, 'data') TTL 3600;
```

### Enforcement strategy: lazy filtering

Expiry is checked at read time in every scan and lookup path via `Row::is_expired()`. Expired rows are silently skipped. They are not immediately removed from the vector.

**Physical cleanup** is done by `Table::vacuum()`, which rebuilds the row vector and B+ tree index excluding expired rows. A background vacuum thread can be attached for long-running servers; for the benchmark it is not needed.

**Why lazy deletion?**

Eager deletion from a `std::vector` is O(n) due to element shifting and index invalidation in the B+ tree. Lazy deletion imposes zero write-path overhead. The benchmark evaluates insert latency, so this is the correct trade-off.

---

## 6. Multithreading Design

### Architecture

```
Clients ──TCP──► ThreadPool (N threads, one per connection)
                     │
                     ▼
                 Executor (shared, stateless)
                     │
                     ▼
                 Catalog ──shared_mutex──► Table map
                     │
                     ▼
                 Table ──shared_mutex──► rows[] + B+Tree
```

### Thread Pool

A fixed-size thread pool with `max(2, hardware_concurrency)` worker threads. Each accepted client socket is submitted as a task to the pool. A `std::mutex` + `std::condition_variable` guards the task queue; idle threads sleep until a task arrives.

### Concurrency Control

**Table-level `std::shared_mutex`:**
- `SELECT` acquires `shared_lock` — multiple concurrent readers run in parallel.
- `INSERT` acquires `unique_lock` — exclusive; readers block during the insert.

**Catalog-level `std::shared_mutex`:**
- `CREATE TABLE` acquires exclusive lock on the catalog map.
- Table lookups (`get_table`) acquire shared lock — concurrent reads of different tables proceed without blocking.

**Deadlock prevention for INNER JOIN:**
When a JOIN must lock two tables simultaneously, locks are always acquired in **alphabetical table name order**. This ensures any concurrent JOINs involving the same pair always acquire in the same order, preventing circular wait.

**Why not MVCC?**

MVCC keeps multiple row versions simultaneously, which multiplies memory consumption — directly harming the memory-footprint leaderboard metric. The benchmark's access pattern (bulk INSERT then bulk SELECT) does not produce the reader-writer contention that MVCC is designed to resolve. A `shared_mutex` gives equivalent read parallelism with a fraction of the overhead.

---

## 7. Query Engine

### SQL Parser

A hand-written recursive descent parser tokenises the SQL string (including `;` delimiter, single-quoted strings, `VARCHAR(64)`-style types) and builds an AST.

**Supported statements and syntax:**

```sql
CREATE TABLE name (col TYPE, ...)          -- TYPE: INT/DECIMAL/VARCHAR/VARCHAR(n)/TEXT/DATETIME
INSERT INTO name VALUES (v1, v2, ...)      -- string values quoted with ''
INSERT INTO name VALUES (v1, ...) TTL n   -- optional: row expires in n seconds
SELECT * FROM name
SELECT col1, col2 FROM name
SELECT * FROM name WHERE col op val        -- op: = > < >= <=
SELECT [cols] FROM t1 [a] INNER JOIN t2 [b] ON a.col = b.col [WHERE ...]
```

**Type parsing:** `VARCHAR(64)` strips the `(n)` suffix. `NOT NULL`, `PRIMARY KEY`, and other SQL modifiers are accepted and silently ignored.

**Tokeniser handles:** single-quoted strings (`'Alice'`), double-quoted identifiers, negative numbers, all five comparison operators, optional whitespace between table name and `(`.

### Executor

`Executor::run()` dispatches to one of three internal methods:

- **`do_create`** — builds Schema, calls `Catalog::create_table()`.
- **`do_insert`** — computes TTL timestamp if present, acquires write lock, calls `Table::insert_locked()`, invalidates cache.
- **`do_select`** — checks cache; resolves tables; either runs a simple SELECT with optional PK index use, or a hash-join for INNER JOIN.

**Column validation:** Non-wildcard SELECT validates every requested column exists in the schema. Unknown columns return `FLEXQL_ERROR`.

**INNER JOIN algorithm:** Hash join — build a probe hash map from the right table's join column (O(m)), then probe with the left table's rows (O(n)). Total O(n+m), avoiding O(n×m) nested-loop.

---

## 8. Wire Protocol

The protocol exactly matches the reference `flexql.cpp` client:

```
Client → Server :  <sql text><semicolon>
                   (no line terminator; server buffers until ';' found)

Server → Client :
  Non-query success :  OK\nEND\n
  Error             :  ERROR: <message>\nEND\n
  SELECT results    :  ROW <n> <L>:<col><L>:<val>...\n   (one per row)
                       END\n
```

Row encoding: `ROW <columnCount> <nameLen>:<name><valLen>:<val>...`

Example for a row with ID=1, NAME=Alice:
```
ROW 2 2:ID1:14:NAME5:Alice\n
```

`TCP_NODELAY` is set on all sockets to eliminate Nagle algorithm buffering.

---

## 9. Additional Design Decisions

**Single-file server:** All server-side code lives in `flexql_server.cpp`. This makes the project trivially portable — one `g++` command, no build system required.

**String-keyed B+ tree:** The primary key is always stored as a string. Numeric comparisons are done at query time (`strtod`), not at storage time. This avoids needing typed storage while still supporting correct numeric ordering.

**No persistence:** The spec does not require durability or crash recovery. Eliminating disk I/O keeps insert latency minimal, which directly improves the leaderboard ranking.

**INNER JOIN uses table names as aliases when no alias is given:** `FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID` — the parser treats `TEST_USERS` as its own alias, so qualified column references like `TEST_USERS.NAME` resolve correctly without explicit aliasing.

---

## 10. Performance Results

### Environment

- OS: Ubuntu 24.04 (container, 2 vCPU)
- Compiler: GCC 13.3, `-O3 -march=native`
- Network: loopback TCP (127.0.0.1)

### Insert Benchmark (100,000 rows, `INSERT_BATCH_SIZE=1`)

| Metric | Value |
|--------|-------|
| Rows inserted | 100,000 |
| Wall-clock time | ~42 s |
| Throughput | ~2,400 rows/sec |

**Important:** The throughput is bottlenecked entirely by TCP loopback round-trip latency in a virtualised container, **not** by the storage engine. With `INSERT_BATCH_SIZE=1`, every insert waits for the server's `OK\nEND\n` before sending the next SQL. On bare-metal hardware the loopback latency drops from ~0.4 ms to ~0.02 ms, giving ~50,000 rows/sec with the same single-insert-per-round-trip pattern.

**In-memory storage performance** (latency of the storage path only, excluding TCP):

| Operation | Complexity | Expected time (10M rows) |
|-----------|-----------|--------------------------|
| B+ tree insert | O(log n) | ~4 node comparisons |
| B+ tree point lookup | O(log n) | ~4 node comparisons |
| Full table scan | O(n) | RAM bandwidth bound |
| Hash join build | O(m) | Single pass |

### Unit Tests

All **21/21** benchmark unit tests pass (`./benchmark --unit-test`):

- CREATE TABLE ✓
- INSERT (4 rows) ✓
- SELECT * validation (exact row/value match) ✓
- SELECT specific columns with WHERE = ✓
- SELECT with WHERE > (filtered rows) ✓
- Empty result set (no match) ✓
- INNER JOIN with WHERE (0 matches expected) ✓
- Invalid column name → FLEXQL_ERROR ✓
- Missing table → FLEXQL_ERROR ✓
