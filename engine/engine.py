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
        self.buffer_pool = BufferPool(capacity=5, disk=self.disk)
        self.redo_log = RedoLog()
        self.undo_log = UndoLog()
        self.tx_table = TransactionTable()
        self.lock_table = LockTable()
        self.doublewrite_buffer = DoublewriteBuffer()
        self.index = BPlusTree(t=2)
        self.next_lsn = 101
        self.next_txid = 1
        self.current_page_id = 1
        self.rows_per_page = 6  # Max rows per page

    def get_page_id(self, row_id: int) -> int | None:
        """
        Get the page_id where a given row is stored.
        Returns None if the row is not found in the index.
        """
        page_id = self.index.get_page_id(row_id)
        return page_id

    def get_row(self, row_id: int) -> tuple:
        """Retrieve a row by its ID."""
        page_id = self.get_page_id(row_id)
        if page_id is None:
            raise Exception(f"Row {row_id} not found")
        
        page = self.buffer_pool.load_page(page_id)
        row = page.rows.get(row_id)
        self.buffer_pool.release_page(page_id)
        
        if row is None:
            raise Exception(f"Row {row_id} not found on page {page_id}")
        
        return row

    def insert_row(self, row: tuple) -> None:
        """
        Insert a new row into the database.
        Implements efficient page allocation - multiple rows per page.
        """
        row_id = row[0]
        
        # Check if row already exists
        existing_page_id = self.get_page_id(row_id)
        if existing_page_id is not None:
            # Update existing row
            self._update_row(row_id, row, existing_page_id)
            return
        
        # Allocate page for new row
        page_id = self._allocate_page_for_row()
        # Load or create page
        try:
            page = self.buffer_pool.load_page(page_id)
        except Exception:
            # Page doesn't exist on disk, create it
            page = Page(page_id=page_id, rows={}, page_lsn=self.next_lsn)
            self.disk.write_page(page)
            self.buffer_pool.add_page_to_memory(page)
            page.pin_count += 1
        
        # Insert row into page
        page.rows[row_id] = row
        self.buffer_pool.mark_dirty(page_id)
        self.buffer_pool.release_page(page_id)
        
        # Update index
        self.index.insert_row_mapping(row_id, page_id)
        
        print(f"Inserted row {row_id} into page {page_id} (page has {len(page.rows)} rows)")

    def _update_row(self, row_id: int, row: tuple, page_id: int) -> None:
        """Update an existing row."""
        page = self.buffer_pool.load_page(page_id)
        page.rows[row_id] = row
        self.buffer_pool.mark_dirty(page_id)
        self.buffer_pool.release_page(page_id)
        print(f"Updated row {row_id} on page {page_id}")

    def _allocate_page_for_row(self) -> int:
        """
        Determine which page to insert a new row into.
        Strategy: Fill current page until full, then create new page.
        """
        # Check if current page exists and has space
        if self.current_page_id in self.buffer_pool.pages:
            current_page = self.buffer_pool.pages[self.current_page_id]
            if len(current_page.page.rows) < self.rows_per_page:
                return self.current_page_id
        
        # Current page is full or doesn't exist, create new page
        if len(self.buffer_pool.pages) > 0:
            self.current_page_id = max(self.buffer_pool.pages.keys()) + 1
        else:
            self.current_page_id = 1
        
        return self.current_page_id

    def shutdown(self) -> None:
        """Clean shutdown: flush all dirty pages and save to disk."""
        print("Shutting down engine...")
        self.buffer_pool.mark_clean_and_flush()
        self.disk.dump_to_json("disk.json")
        self.index.dump_to_json("index.json")
        print("✓ All dirty pages flushed")
        print("✓ Data saved to disk.json and index.json")

    def print_stats(self) -> None:
        """Print database statistics."""
        total_rows = sum(len(page.rows) for page in self.disk.pages.values())
        print(f"\n=== Database Statistics ===")
        print(f"Total pages: {len(self.disk.pages)}")
        print(f"Total rows: {total_rows}")
        print(f"Average rows per page: {total_rows / len(self.disk.pages) if self.disk.pages else 0:.2f}")
        print(f"Max rows per page: {self.rows_per_page}")
        print(f"Buffer pool capacity: {self.buffer_pool.capacity}")
        print(f"Pages currently in buffer: {len(self.buffer_pool.pages)}")