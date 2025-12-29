from pydantic import BaseModel
from threading import Lock
from typing import Optional, Any
from enum import Enum

from memory.undo_record import UndoRecord, UndoRecordModel
from memory.redo_record import RedoRecord, RedoLogRecordModel
from memory.locks import LockTable

class TransactionStatus(Enum):
    ACTIVE = "active"
    PREPARING = "preparing"
    COMMITTED = "committed"
    ABORTED = "aborted"


class TransactionTableEntry(BaseModel):
    txid: int
    status: str
    
    class Config:
        arbitrary_types_allowed = True


class TransactionTable:
    """
    Transaction Table - tracks all active and recent transactions
    """
    
    def __init__(self):
        self.active: dict[int, TransactionTableEntry] = {}
        self.lock = Lock()
    
    def register_transaction(self, entry: TransactionTableEntry) -> None:
        with self.lock:
            self.active[entry.txid] = entry
    
    def commit_transaction(self, txid: int) -> None:
        with self.lock:
            if txid in self.active:
                self.active[txid].status = TransactionStatus.COMMITTED.value
    
    def rollback_transaction(self, txid: int) -> None:
        with self.lock:
            if txid in self.active:
                self.active[txid].status = TransactionStatus.ABORTED.value
    
    def get_transaction(self, txid: int) -> Optional[TransactionTableEntry]:
        with self.lock:
            return self.active.get(txid)
    
    def cleanup_transaction(self, txid: int) -> None:
        with self.lock:
            if txid in self.active:
                del self.active[txid]
    
    def get_active_transactions(self) -> list[int]:
        with self.lock:
            return [
                txid for txid, entry in self.active.items()
                if entry.status == TransactionStatus.ACTIVE.value
            ]

class Transaction:
    """
    Transaction class implementing ACID properties:
    - Atomicity: All operations succeed or all fail (using undo log)
    - Consistency: Database remains in valid state
    - Isolation: Row-level locking (2PL - Two-Phase Locking)
    - Durability: Write-Ahead Logging (WAL) with redo logs
    """
    
    def __init__(self, txid: int, next_lsn: int, tx_table: TransactionTable, lock_table: LockTable, redo_record: RedoRecord, undo_record: UndoRecord, operation: Any):
        self.txid = txid
        self.next_lsn = next_lsn
        self.tx_table = tx_table
        self.lock_table = lock_table
        self.redo_record = redo_record
        self.undo_record = undo_record
        self.operation = operation
        self.status = TransactionStatus.ACTIVE
        self.locked_rows: set[int] = set()
        
    def begin(self) -> None:
        print(f"[TX-{self.txid}] BEGIN transaction")
        entry = TransactionTableEntry(
            txid=self.txid,
            status=TransactionStatus.ACTIVE.value,
        )
        self.tx_table.register_transaction(entry)
        
    def acquire_lock(self, row_id: int) -> bool:
        if self.lock_table.acquire_lock(self.txid, row_id):
            self.locked_rows.add(row_id)
            print(f"[TX-{self.txid}] Acquired lock on row {row_id}")
            return True
        print(f"[TX-{self.txid}] Failed to acquire lock on row {row_id}")
        return False
    
    def release_locks(self) -> None:
        for row_id in self.locked_rows:
            self.lock_table.release_lock(self.txid, row_id)
            print(f"[TX-{self.txid}] Released lock on row {row_id}")
        self.locked_rows.clear()
    
    def add_undo_record(self, record: UndoRecordModel) -> None:
        self.undo_record.append(record)
        
    def add_redo_record(self, record: RedoLogRecordModel) -> None:
        self.redo_record.append(record)
    
    def commit(self) -> None:
        """
        Commit transaction:
        1. Flush redo log (WAL - Write-Ahead Logging)
        2. Mark transaction as committed
        3. Release all locks
        """
        if self.status != TransactionStatus.ACTIVE:
            raise Exception(f"Cannot commit transaction in {self.status} state")
        
        print(f"[TX-{self.txid}] COMMIT transaction")
        
        # Phase 1: Prepare - flush redo log to ensure durability
        self.status = TransactionStatus.PREPARING
        if self.redo_record.redo_lsns:
            self.redo_record.flush()
            print(f"[TX-{self.txid}] Flushed redo log (WAL)")
        
        # Phase 2: Commit
        self.status = TransactionStatus.COMMITTED
        self.tx_table.commit_transaction(self.txid)
        
        # Phase 3: Release locks
        self.release_locks()
        
        self.undo_record.clear()
        self.redo_record.clear()
        print(f"[TX-{self.txid}] Transaction committed successfully")
    
    def rollback(self) -> None:
        """
        Rollback transaction:
        1. Apply undo records in reverse order
        2. Mark transaction as aborted
        3. Release all locks
        """
        if self.status not in [TransactionStatus.ACTIVE, TransactionStatus.PREPARING]:
            raise Exception(f"Cannot rollback transaction in {self.status} state")
        
        print(f"[TX-{self.txid}] ROLLBACK transaction")
        
        # Apply undo records in reverse order
        for undo_record in reversed(self.undo_record.records):
            self._apply_undo_record(undo_record)
        
        # Mark as aborted
        self.status = TransactionStatus.ABORTED
        self.tx_table.rollback_transaction(self.txid)
        
        # Release all locks
        self.release_locks()
        
        print(f"[TX-{self.txid}] Transaction rolled back successfully")
    
    def _apply_undo_record(self, undo_record: UndoRecord) -> None:
        print(f"[TX-{self.txid}] Applying undo: {undo_record.operation} on row {undo_record.row_id}")
        
        if undo_record.operation == "INSERT":
            # Undo INSERT: Delete the row
            self.operation.delete_row(undo_record.row_id, undo_record.page_id)
        elif undo_record.operation == "UPDATE":
            # Undo UPDATE: Restore old value
            self.operation.update_row(
                undo_record.row_id, 
                undo_record.old_value,
                undo_record.page_id
            )
        elif undo_record.operation == "DELETE":
            # Undo DELETE: Re-insert the row
            self.operation.insert_row(
                undo_record.old_value,
                self.next_lsn
            )
