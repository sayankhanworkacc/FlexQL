# FlexQL

A flexible SQL-like in-memory database driver — client-server system implemented entirely in C++17.

> **Design Document:** [DESIGN.md](DESIGN.md)  
> **Repository:** https://github.com/sayankhanworkacc/FlexQL

---

## Prerequisites

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install -y g++ make

# Fedora / RHEL
sudo dnf install -y gcc-c++

# macOS (Homebrew)
brew install gcc
```

No external database libraries required. The entire system is self-contained.

---

## Build

```bash
sh compile.sh
```

Produces two binaries:
- `./server` — the FlexQL database server
- `./benchmark` — the benchmark and unit test client

---

## Run

**Always start the server first.**

### Terminal 1 — Start the server

```bash
./server
# FlexQL Server running on port 9000 (N threads)
```

### Terminal 2 — Run unit tests

```bash
./benchmark --unit-test
```

Expected output: `Unit Test Summary: 21/21 passed, 0 failed.`

### Terminal 2 — Run performance benchmark

```bash
# Default: 10 rows (quick smoke test)
./benchmark

# Custom row count
./benchmark 100000
./benchmark 1000000
./benchmark 10000000
```

### Terminal 2 — Interactive REPL (optional)

You can also use any tool that speaks TCP, for example:
```bash
# Using netcat
nc 127.0.0.1 9000
CREATE TABLE t (id INT, name VARCHAR);
INSERT INTO t VALUES (1, 'Alice');
SELECT * FROM t;
```

---

## SQL Reference

### CREATE TABLE

```sql
CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);
```

Supported types: `INT`, `DECIMAL`, `VARCHAR`, `VARCHAR(n)`, `TEXT`, `DATETIME`

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);
INSERT INTO table_name VALUES (val1, val2, ...) TTL seconds;
```

String values can be quoted (`'Alice'`) or unquoted. `TTL` sets an expiration time — expired rows become invisible to SELECT.

### SELECT

```sql
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name;
SELECT * FROM table_name WHERE col op value;
SELECT * FROM t1 INNER JOIN t2 ON t1.col = t2.col;
SELECT * FROM t1 a INNER JOIN t2 b ON a.col = b.col WHERE b.col op value;
```

WHERE operators: `=`, `>`, `<`, `>=`, `<=`

---

## File Structure

```
FlexQL/
├── flexql.h               ← Public C API
├── flexql.cpp             ← Client library
├── benchmark_flexql.cpp   ← Benchmark + unit tests
├── flexql_server.cpp      ← Server implementation
├── compile.sh             ← Build script
├── DESIGN.md              ← Architecture and design decisions
└── README.md
```

---

## C API Reference

```c
#include "flexql.h"

/* Connect to server */
FlexQL *db;
flexql_open("127.0.0.1", 9000, &db);

/* Execute non-query SQL */
flexql_exec(db, "CREATE TABLE t (id INT, name VARCHAR);", NULL, NULL, NULL);
flexql_exec(db, "INSERT INTO t VALUES (1, 'Alice');", NULL, NULL, NULL);

/* Execute query with callback */
int my_callback(void *data, int n, char **values, char **col_names) {
    for (int i = 0; i < n; i++)
        printf("%s = %s\n", col_names[i], values[i]);
    return 0; /* return 1 to abort */
}

char *err = NULL;
flexql_exec(db, "SELECT * FROM t;", my_callback, NULL, &err);
if (err) { fprintf(stderr, "Error: %s\n", err); flexql_free(err); }

/* Disconnect */
flexql_close(db);
```

---

## Design Highlights

| Component | Choice | Reason |
|-----------|--------|--------|
| Storage | Row-major `vector<Row>` | Matches row-callback delivery model |
| Index | B+ Tree (fan-out=128) | Handles `=`,`>`,`<`,`>=`,`<=` in O(log n) |
| Cache | LRU (8192 entries) | Simple, correct, invalidated on INSERT |
| Concurrency | `shared_mutex` per table | Readers run in parallel; no MVCC bloat |
| Threading | Fixed thread pool | One thread per connected client |
| Expiry | Lazy filter at read time | Zero write-path overhead |
| Persistence | None (pure in-memory) | Not required; keeps insert latency minimal |

See [DESIGN.md](DESIGN.md) for full details.

---

## Troubleshooting

**`Cannot open FlexQL`** — Make sure `./server` is running before starting the benchmark.

**Port already in use** — Kill any existing server: `pkill server` or `kill $(lsof -t -i:9000)`

**Slow insert throughput** — The benchmark uses `INSERT_BATCH_SIZE=1`, so each insert is a separate TCP round-trip. This is expected; the bottleneck is network latency, not the storage engine.
