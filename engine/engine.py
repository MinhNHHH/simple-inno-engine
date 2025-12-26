from memory.disks import Disk
from memory.pages import Page
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