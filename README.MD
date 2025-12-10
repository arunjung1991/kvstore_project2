# KVStore v2: Transactions, TTL, Range, and Multi-Ops

This project extends the Part 1 key–value store with:

- **Transactions** (`BEGIN` / `COMMIT` / `ABORT`)
- **TTL in milliseconds** (`EXPIRE`, `TTL`, `PERSIST`)
- **Multi-key operations** (`MSET`, `MGET`)
- **Range queries** over primary keys (`RANGE`)
- All while keeping **append-only persistence** and a strict **STDIN/STDOUT** CLI suitable for black-box testing (Gradebot project-2).

The core design is still:

- An **append-only log** (`data.db`) for durability.
- A custom **B+ tree index** for ordered key lookups.
- An in-memory **TTL table** and **tombstones** for deletes/expiration.
- A **transaction overlay** that buffers writes until `COMMIT`.

---

## Project Structure

```text
KVStore/
├── engine.py             # Coordinates index, TTL, tombstones, log replay
├── index.py              # B+ Tree implementation (no built-in dict/map for store)
├── storage.py            # Append-only log (fsync per write; EXPIREAT/PERSIST support)
├── main.py               # CLI, command parsing, transactions, multi-ops, range
├── README.md             # This documentation
└── data.db               # Log file created at runtime (default)
```

---

## Features (Project 2 Enhancements)

### ✅ Transactions
- Supports `BEGIN`, `COMMIT`, and `ABORT`
- No nested transactions
- Buffered writes applied atomically
- Crash-safe (no uncommitted changes survive restart)

### ✅ TTL (ms)
- EXPIRE assigns absolute expiration time
- TTL returns remaining lifetime or status code
- PERSIST removes TTL
- Lazy expiration (checked on reads)

### ✅ Multi-Key Operations
- MSET sets multiple keys
- MGET retrieves multiple keys

### ✅ Range Queries
- Lexicographic order
- Inclusive bounds, open-ended supported
- Skips expired/deleted/internal keys

### ✅ Persistence
- SET, DEL, EXPIREAT, PERSIST logged
- Log replay reconstructs full state

### ✅ Custom B+ Tree Index
- Ordered key storage
- Efficient range scans
- No built‑in dict/map used for main store

---

## How to Run the KVStore

```bash
python3 main.py
```

To specify a custom log (Gradebot):

```bash
python3 main.py gradebot.db
```

---

## Gradebot Usage

```bash
~/gradebot project-2 --dir /home/UNT/ak2102/Database/kvstore_project --run "python3 main.py gradebot.db"
```

---

## Example Sessions

### Basic SET / GET
```
SET a 10
OK
GET a
10
```

### Multi-Key
```
MSET x 1 y 2
OK
MGET x y
1
2
```

### TTL
```
SET t 42
OK
EXPIRE t 100
1
GET t
nil
TTL t
-2
```

### Transactions
```
BEGIN
SET a 99
COMMIT
OK
```

### RANGE
```
RANGE b d
b
c
d
END
```
