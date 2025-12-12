from memory.disks import Disk
from memory.pages import Page
from memory.buffer_pool import BufferPool
from memory.redo_log import RedoLog
from memory.undo_log import UndoLog
from memory.transactions import TransactionTable
from memory.locks import LockTable
from memory.double_write_buffer import DoublewriteBuffer
from memory.index import BPlusTree

class InnoEngine:
    def __init__(self):
        self.disk = Disk()
        self.buffer_pool = BufferPool(capacity=2, disk=self.disk)
        self.redo_log = RedoLog()
        self.undo_log = UndoLog()
        self.tx_table = TransactionTable()
        self.lock_table = LockTable()
        self.doublewrite_buffer = DoublewriteBuffer()
        self.index = BPlusTree(t=2)
        self.next_lsn = 101
        self.next_txid = 1

    def get_page_id(self, row_id: int) -> int:
        node, page_id = self.index.search(self.index.root, row_id)
        print("node, [get_page_id]", node, page_id)
        if node is None:
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
        """
        Insert a new row into the database:
        - Insert the row's key into the B+Tree index.
        - If the row doesn't exist, create a new page and add it to disk and buffer.
        - Otherwise, load the existing page and insert/update the row.
        """
        row_id = row[0]
        # Insert the row_id into the index
        self.index.insert(row_id)
        # Find which page_id it now maps to
        page_id = self.get_page_id(row_id)
        if page_id is None:
            # Page does not exist, create new
            page_id = len(self.disk.pages) + 1
            page = Page(page_id=page_id, rows=[row], page_lsn=self.next_lsn)
            self.disk.write_page(page)
            self.buffer_pool.add_page_to_memory(page)
            print(f"Inserted new row {row} into new page {page_id}")
        else:
            # Page exists, load it and upsert the row
            try:
                page = self.buffer_pool.load_page(page_id)
            except Exception as e:
                # If the page is not found on disk (e.g. Exception from get_page), create it now
                print(f"Page {page_id} not found on disk, creating new page. Reason: {e}")
                page = Page(page_id=page_id, rows=[row], page_lsn=self.next_lsn)
                self.disk.write_page(page)
                self.buffer_pool.add_page_to_memory(page)
                print(f"Inserted new row {row} into new page {page_id}")
                return  # finished insertion
            page.rows[row_id] = row
            print(f"Inserted/updated row {row} in existing page {page_id}")
            self.buffer_pool.mark_dirty(page_id)
            self.buffer_pool.release_page(page_id)
            self.disk.write_page(page)
        # flush all dirty pages to disk
        self.buffer_pool.mark_clean_and_flush()
        self.disk.dump_to_json("disk.json")