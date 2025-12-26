from memory.undo_record import UndoRecordModel
from memory.disks import Disk
from memory.buffer_pool import BufferPool
from memory.redo_record import RedoRecord, RedoLogRecordModel
from memory.undo_record import UndoRecord
from memory.transactions import TransactionTable, Transaction
from memory.locks import LockTable
from memory.double_write_buffer import DoublewriteBuffer
from memory.index import BPlusTree

from engine.operation import Operation

class InnoEngine:
    def __init__(self, index: BPlusTree):
        self.disk = Disk()
        self.buffer_pool = BufferPool(capacity=5, disk=self.disk)
        self.index = index
        self.operation = Operation(
            buffer_pool=self.buffer_pool,
            index=self.index,
            disk=self.disk
        )
        self.redo_record = RedoRecord()
        self.undo_record = UndoRecord()
        self.tx_table = TransactionTable()
        self.lock_table = LockTable()
        self.doublewrite_buffer = DoublewriteBuffer()
        self.next_txid = 1
        self.next_lsn = 1

    def get_row(self, row_id: int) -> tuple:
        return self.operation.get_row(row_id)

    def insert_row(self, row: tuple) -> None:
        self.operation.insert_row(row)

    def shutdown(self) -> None:
        self.operation.shutdown()

    def print_stats(self) -> None:
        """Print database statistics."""
        total_rows = sum(len(page.rows) for page in self.disk.pages.values())
        print(f"\n=== Database Statistics ===")
        print(f"Total pages: {len(self.disk.pages)}")
        print(f"Total rows: {total_rows}")
        print(f"Average rows per page: {total_rows / len(self.disk.pages) if self.disk.pages else 0:.2f}")
        print(f"Buffer pool capacity: {self.buffer_pool.capacity}")
        print(f"Pages currently in buffer: {len(self.buffer_pool.pages)}")
    
    # ==================== Transaction Methods ====================
    def begin_transaction(self) -> Transaction:
        """
        Begin a new transaction
        Returns a Transaction object that can be used to perform operations
        """
        txid = self.next_txid
        self.next_txid += 1
        
        tx = Transaction(txid=txid, tx_table=self.tx_table, lock_table=self.lock_table, undo_record=self.undo_record, redo_record=self.redo_record, operation=self.operation)
        tx.begin()
        return tx
    
    def tx_insert_row(self, tx: Transaction, row: tuple) -> None:
        """
        Insert a row within a transaction
        - Acquires lock on the row
        - Creates undo record for rollback
        - Writes redo log for durability
        """
        row_id = row[0]
        
        # Check if row already exists
        existing_page_id = self.operation.get_page_id(row_id)
        if existing_page_id is not None:
            raise Exception(f"Row {row_id} already exists. Use tx_update_row instead.")
        
        # Acquire lock on the row
        if not tx.acquire_lock(row_id):
            raise Exception(f"Failed to acquire lock on row {row_id}")
        
        page_id = self.operation.allocate_page_for_row()
        
        record_data = {"row_id": row_id, "row": row}
        lsn = self.next_lsn
        self.next_lsn += 1
        self._create_undo_redo_log(tx, row_id, page_id, "INSERT", lsn, record_data, None)
        # Perform the actual insert
        self.operation.insert_row(row)
        
        print(f"[TX-{tx.txid}] Inserted row {row_id} into page {page_id}")
    
    def tx_update_row(self, tx: Transaction, row_id: int, new_row: tuple) -> None:
        """
        Update a row within a transaction
        - Acquires lock on the row
        - Creates undo record with old value for rollback
        - Writes redo log for durability
        """
        # Get current row and page
        page_id = self.operation.get_page_id(row_id)
        if page_id is None:
            raise Exception(f"Row {row_id} not found")
        
        # Acquire lock on the row
        if not tx.acquire_lock(row_id):
            raise Exception(f"Failed to acquire lock on row {row_id}")
        
        # Get old value for undo log
        old_row = self.operation.get_row(row_id)
        
        record_data = {"row_id": row_id, "old_row": old_row, "new_row": new_row}
        lsn = self.next_lsn
        self.next_lsn += 1
        self._create_undo_redo_log(tx, row_id, page_id, "UPDATE", lsn, record_data, old_row)
        # Perform the actual update
        self.operation.update_row(row_id, new_row, page_id)
        
        print(f"[TX-{tx.txid}] Updated row {row_id} on page {page_id}")
    
    def tx_delete_row(self, tx: Transaction, row_id: int) -> None:
        """
        Delete a row within a transaction
        - Acquires lock on the row
        - Creates undo record with old value for rollback
        - Writes redo log for durability
        """
        # Get current row and page
        page_id = self.operation.get_page_id(row_id)
        if page_id is None:
            raise Exception(f"Row {row_id} not found")
        
        # Acquire lock on the row
        if not tx.acquire_lock(row_id):
            raise Exception(f"Failed to acquire lock on row {row_id}")
        
        # Get old value for undo log
        old_row = self.get_row(row_id)
        
        record_data = {"row_id": row_id, "old_row": old_row}
        lsn = self.next_lsn
        self.next_lsn += 1
        self._create_undo_redo_log(tx, row_id, page_id, "DELETE", lsn, record_data, old_row)
        # Perform the actual delete
        self.operation.delete_row(row_id, page_id)
        
        print(f"[TX-{tx.txid}] Deleted row {row_id} from page {page_id}")
    
    def _create_undo_redo_log(self, 
        tx: Transaction, 
        row_id: int,  
        page_id: int, 
        action: str, 
        lsn: int,
        data: dict,
        old_value: tuple) -> None:
        
        undo_record = UndoRecordModel(
            row_id=row_id,
            page_id=page_id,
            old_value=old_value,
            operation=action
        )
        tx.add_undo_record(undo_record)

        redo_record = RedoLogRecordModel(
            lsn=lsn,
            txid=tx.txid,
            action=action,
            data=data,
            page_id=page_id
        )
        tx.add_redo_record(redo_record)