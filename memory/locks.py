from threading import Lock
from typing import Optional
import time


class LockType:
    """Lock types for concurrency control"""
    SHARED = "S"      # Read lock
    EXCLUSIVE = "X"   # Write lock


class RowLock:
    """Represents a lock on a single row"""
    def __init__(self, row_id: int, txid: int, lock_type: str):
        self.row_id = row_id
        self.txid = txid
        self.lock_type = lock_type


class LockTable:
    """
    Lock Table for managing row-level locks
    Implements Two-Phase Locking (2PL) for transaction isolation
    """
    
    def __init__(self):
        self.locks: dict[int, RowLock] = {}  # row_id -> RowLock
        self.lock = Lock()  # Thread-safe access
    
    def acquire_lock(self, txid: int, row_id: int, lock_type: str = LockType.EXCLUSIVE) -> bool:
        """
        Acquire a lock on a row
        Returns True if lock acquired, False if lock held by another transaction
        """
        with self.lock:
            if row_id in self.locks:
                existing_lock = self.locks[row_id]
                
                # If same transaction, allow (re-entrant lock)
                if existing_lock.txid == txid:
                    return True
                
                # Row is locked by another transaction
                return False
            
            # Acquire the lock
            self.locks[row_id] = RowLock(row_id, txid, lock_type)
            return True
    
    def release_lock(self, txid: int, row_id: int) -> None:
        with self.lock:
            if row_id in self.locks:
                existing_lock = self.locks[row_id]
                if existing_lock.txid == txid:
                    del self.locks[row_id]
    
    def release_all_locks(self, txid: int) -> None:
        with self.lock:
            rows_to_release = [
                row_id for row_id, lock in self.locks.items()
                if lock.txid == txid
            ]
            for row_id in rows_to_release:
                del self.locks[row_id]
    
    def is_locked(self, row_id: int) -> bool:
        with self.lock:
            return row_id in self.locks
    
    def get_lock_holder(self, row_id: int) -> Optional[int]:
        with self.lock:
            if row_id in self.locks:
                return self.locks[row_id].txid
            return None
