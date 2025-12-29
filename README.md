# 🗄️ Simple InnoDB Storage Engine

A simplified educational implementation of MySQL's InnoDB storage engine in Python, demonstrating core database internals and ACID transaction properties.

## 📖 Overview

This project implements the fundamental concepts of a transactional storage engine, providing hands-on understanding of how modern databases work internally. It replicates key InnoDB features including crash recovery, transaction management, and efficient data storage.

## 🎯 Core Concepts Implemented

### **1. ACID Transaction Properties**

#### **Atomicity** - All or Nothing
- **Undo Logs**: Each transaction maintains independent undo logs to track old values
- **Rollback**: Applies undo records in reverse order to restore previous state
- Transactions either complete fully or have no effect

#### **Consistency** - Valid State Maintenance
- Data validation before operations
- Constraint enforcement (unique row_id)
- Index consistency with B+Tree

#### **Isolation** - Concurrent Transaction Safety
- **Two-Phase Locking (2PL)**: Row-level exclusive locks
- Lock acquisition before modifications
- Locks held until commit/rollback (strict 2PL)
- Deadlock prevention through lock ordering

#### **Durability** - Crash Resistance
- **Write-Ahead Logging (WAL)**: Redo logs written before data modifications
- **Double Write Buffer**: Protects against torn page writes
- Redo log flush on transaction commit

---

### **2. Buffer Pool Management**

**LRU Cache Implementation**
```
┌────────────────────────────────────┐
│   Buffer Pool (In-Memory Cache)    │
├────────────────────────────────────┤
│  • LRU eviction policy             │
│  • Pin/Unpin mechanism             │
│  • Dirty page tracking             │
│  • Thread-safe operations          │
└────────────────────────────────────┘
```

**Features:**
- **Least Recently Used (LRU)**: Evicts cold pages efficiently
- **Pinning**: Prevents eviction of active pages
- **Dirty Tracking**: Identifies modified pages needing flush
- **Doubly-Linked List**: O(1) LRU operations

---

### **3. Double Write Buffer (Crash Safety)**

**Protection Against Torn Page Writes**

```
Normal Write Flow:
┌──────────────┐     ┌──────────────────┐     ┌─────────────┐
│ Modify Page  │ →  │ Write to DWB     │ →  │ Write to    │
│ in Memory    │     │ (Sequential)     │     │ Actual Disk │
└──────────────┘     └──────────────────┘     └─────────────┘
                             ↓
                     ⚡ Crash Here?
                     → Recovery uses DWB copy!
```

**How It Works:**
1. **Phase 1**: Write complete pages to sequential DWB area (`doublewrite_buffer.json`)
2. **Phase 2**: Write pages to their actual scattered locations (`disk.json`)
3. **Phase 3**: Clear DWB staging area
4. **Recovery**: If crash during Phase 2, restore from DWB

---

### **4. Write-Ahead Logging (WAL)**

**Redo Logs for Durability**

```
Transaction Flow:
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Modify     │ →  │ Write Redo   │ →  │ Modify Page  │
│  In Memory  │     │ Log (WAL)    │     │ In Buffer    │
└─────────────┘     └──────────────┘     └──────────────┘
                           ↓
                    Commit Point
```

**Guarantee**: Redo log persisted BEFORE data modifications (WAL principle)

---

### **5. Index Management**

**B+Tree Index**
- Maps `row_id` → `page_id` for fast lookups
- Efficient range queries
- Maintains sorted order
- Persistent storage with JSON serialization

---

### **6. Concurrency Control**

**Two-Phase Locking Protocol**

```
Growing Phase          Shrinking Phase
├──────────────┐      ┌──────────────┤
│ Acquire locks │  →  │ Release locks │
│ No releases   │      │ No more locks │
└──────────────┘      └──────────────┘
        ↑                      ↑
   Operations           Commit/Rollback
```

**Lock Manager:**
- Row-level exclusive locks
- Deadlock prevention
- Re-entrant lock support
- Automatic lock release on transaction end

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     InnoEngine                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │            Transaction Manager                    │  │
│  │  • Begin/Commit/Rollback                         │  │
│  │  • Undo/Redo Logs (per-transaction)              │  │
│  │  • LSN (Log Sequence Number) Tracking            │  │
│  └─────────────────────┬────────────────────────────┘  │
│                        │                                │
│  ┌─────────────────────▼────────────────────────────┐  │
│  │            Buffer Pool (LRU Cache)                │  │
│  │  • In-memory page cache                          │  │
│  │  • Dirty page management                         │  │
│  │  • Pin/Unpin for active pages                    │  │
│  └─────────────────────┬────────────────────────────┘  │
│                        │                                │
│  ┌─────────────────────▼────────────────────────────┐  │
│  │         Double Write Buffer                       │  │
│  │  • Prevents torn pages                           │  │
│  │  • Sequential write area                         │  │
│  │  • Crash recovery support                        │  │
│  └─────────────────────┬────────────────────────────┘  │
│                        │                                │
│  ┌─────────────────────▼────────────────────────────┐  │
│  │                Disk Storage                       │  │
│  │  • Page persistence (disk.json)                  │  │
│  │  • Deep copy semantics                           │  │
│  │  • Page-level granularity                        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌────────────────┐  ┌────────────────┐               │
│  │  Lock Table    │  │  B+Tree Index  │               │
│  │  (2PL)         │  │  (row→page)    │               │
│  └────────────────┘  └────────────────┘               │
└─────────────────────────────────────────────────────────┘
```

---

## 💾 Storage Files

| File | Purpose | Lifetime |
|------|---------|----------|
| **`disk.json`** | Actual database pages | Permanent |
| **`doublewrite_buffer.json`** | DWB crash recovery area | Temporary |
| **`index.json`** | B+Tree row→page mapping | Permanent |
| **`redo_log.json`** | Write-ahead logs | Until applied |

---

## 🔄 Data Flow Example

### **Transaction Insert Operation**

```python
# User code
tx = engine.begin_transaction()
engine.tx_insert_row(tx, (1, "Alice", 25))
tx.commit()
```

**Internal Flow:**

```
1. Begin Transaction
   └─ Assign TXID
   └─ Create per-transaction undo/redo logs
   └─ Register in transaction table

2. Insert Row
   └─ Acquire exclusive lock on row_id=1
   └─ Create undo log entry (for rollback)
   └─ Create redo log entry (for durability)
   └─ Allocate page for row
   └─ Load page into buffer pool
   └─ Modify page in memory
   └─ Mark page as dirty
   └─ Update B+Tree index

3. Commit
   └─ Flush redo log to disk (WAL)
   └─ Mark transaction as COMMITTED
   └─ Release all locks
   └─ Clear undo log (no longer needed)

4. Background Checkpoint (later)
   └─ Write dirty pages to DWB
   └─ fsync DWB area
   └─ Write pages to actual disk locations
   └─ Clear DWB
   └─ Persist disk.json and index.json
```

---

## 🎓 Key Learning Points

### **1. Page-Based Storage**
- Database stored as fixed-size pages (not individual rows)
- Multiple rows per page for efficiency
- Pages are unit of I/O

### **2. Buffer Pool as Performance Multiplier**
- Avoids expensive disk I/O
- LRU keeps hot data in memory
- Write coalescing (multiple updates = one write)

### **3. Crash Recovery**
- **Redo Logs**: Replay committed transactions after crash
- **Undo Logs**: Rollback uncommitted transactions after crash
- **Double Write Buffer**: Detect and fix torn pages

### **4. Transaction Isolation**
- Each transaction gets independent undo/redo logs
- Locks prevent concurrent modifications
- Strict 2PL guarantees serializability

### **5. Write-Ahead Logging**
- Log written BEFORE data modified
- Guarantees durability without immediate disk writes
- Enables fast commits

---

## 📊 Complexity Analysis

| Operation | Time Complexity | Notes |
|-----------|----------------|--------|
| **Insert** | O(log n) | B+Tree insert + buffer pool |
| **Read** | O(log n) | B+Tree lookup + buffer pool |
| **Update** | O(log n) | Same as insert |
| **Delete** | O(log n) | B+Tree delete + buffer pool |
| **Commit** | O(k) | k = number of modified pages |
| **LRU Eviction** | O(1) | Doubly-linked list |
| **Lock Acquire** | O(1) | Hash table lookup |

---

## 🚀 Usage Example

```python
from engine.engine import InnoEngine
from memory.index import BPlusTree

# Initialize engine
index = BPlusTree(t=3)
engine = InnoEngine(index=index)

# Start transaction
tx = engine.begin_transaction()

# Perform operations
engine.tx_insert_row(tx, (1, "Alice", 25))
engine.tx_update_row(tx, 1, (1, "Alice", 26))

# Commit (ACID guarantees)
tx.commit()

# Checkpoint (flush to disk)
engine.checkpoint()
```

---

## 🔍 Educational Value

This implementation demonstrates:
- ✅ How databases prevent data loss
- ✅ Why transactions are atomic
- ✅ How buffer pools improve performance
- ✅ How crash recovery works
- ✅ Why WAL is fundamental to durability
- ✅ How concurrency control prevents conflicts

---

## ⚠️ Limitations (Educational Simplifications)

1. **No MVCC**: Uses 2PL instead of Multi-Version Concurrency Control
2. **JSON Storage**: Production uses binary formats
3. **Single Node**: No distribution or replication
4. **No Compression**: Pages stored uncompressed
5. **Simplified Recovery**: Full recovery process more complex
6. **No Checksums**: Production validates page integrity

---

## 📚 Concepts Demonstrated

| Concept | Implementation |
|---------|----------------|
| **ACID** | Transactions with undo/redo logs |
| **Buffer Management** | LRU cache with pinning |
| **Crash Recovery** | Double write buffer + WAL |
| **Concurrency** | Two-phase locking |
| **Indexing** | B+Tree for row→page mapping |
| **Durability** | Write-ahead logging |
| **Isolation** | Per-transaction undo/redo logs |

---

## 🎯 Project Status

**Grade**: Educational Implementation (B+)
- ✅ Core concepts correctly implemented
- ✅ Clean architecture and code quality
- ✅ Thread-safe operations
- ⚠️ Not production-ready (by design)

---

## 📖 References

This implementation is inspired by:
- MySQL InnoDB Storage Engine
- PostgreSQL MVCC
- SQLite WAL mode
- Database System Concepts (Silberschatz)

---

**Built for learning database internals** 🎓
