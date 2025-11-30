from memory.disks import Disk
from memory.pages import Page
from memory.buffer_pool import BufferPool
from memory.redo_log import RedoLog
from memory.undo_log import UndoLog
from memory.transactions import TransactionTable
from memory.locks import LockTable
from memory.double_write_buffer import DoublewriteBuffer
from memory.index import BTree

class InnoEngine:
    def __init__(self):
        self.disk = Disk()
        self.buffer_pool = BufferPool(capacity=2, disk=self.disk)
        self.redo_log = RedoLog()
        self.undo_log = UndoLog()
        self.tx_table = TransactionTable()
        self.lock_table = LockTable()
        self.doublewrite_buffer = DoublewriteBuffer()
        self.index = BTree(t=2)
        self.next_lsn = 101
        self.next_txid = 1

    def get_page_id(self, row_id: int) -> int:
        _, page_id = self.index.search(self.index.root, row_id)
        if page_id is None:
            return None
        return page_id

    def get_row(self, row_id: int) -> tuple:
        page_id = self.get_page_id(row_id)
        if page_id is None:
            raise Exception(f"Row {row_id} not found")
        page = self.buffer_pool.load_page(page_id)
        row = page.rows[row_id]
        self.buffer_pool.release_page(page_id)
        return row

    def insert_page(self, row: tuple) -> None:
        page_id = self.get_page_id(row[0])
        if page_id is None:
            page_id = len(self.disk.pages) + 1
            page = Page(page_id=page_id, rows=[row], page_lsn=self.next_lsn)
            self.disk.write_page(page)
            self.index.insert(page_id)
            self.buffer_pool.add_page_to_memory(page)
            print(f"Inserted row {row} into page {page_id}")
            # self.redo_log.add_record(page_id, page)
            # self.undo_log.add_record(page_id, page)
        else:
            page = self.buffer_pool.load_page(page_id)
            page.rows[row[0]] = row