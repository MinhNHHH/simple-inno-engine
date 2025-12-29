from threading import Lock

from memory.disks import Disk
from memory.buffer_pool import BufferPool
from memory.index import BPlusTree
from memory.pages import Page

class Operation:
    def __init__(self, disk: Disk, buffer_pool: BufferPool, index: BPlusTree):
        self.disk = disk
        self.buffer_pool = buffer_pool
        self.index = index
        self.current_page_id = disk.get_current_page_id()
        self.rows_per_page = 10  # Max rows per page
        self.lock = Lock()

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
    
    def insert_row(self, row: tuple, next_lsn: int) -> None:
        """
        Insert a new row into the database.
        Implements efficient page allocation - multiple rows per page.
        """
        row_id = row[0]
        with self.lock:
            # Check if row already exists
            existing_page_id = self.get_page_id(row_id)
            if existing_page_id is not None:
                # Update existing row
                self.update_row(row_id, row, existing_page_id)
                return
        
            # Allocate page for new row
            page_id = self.allocate_page_for_row()
            # Load or create page
            try:
                page = self.buffer_pool.load_page(page_id)
            except Exception:
                # Page doesn't exist on disk, create it
                page = Page(page_id=page_id, rows={}, page_lsn=next_lsn)
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

    def delete_row(self, row_id: int, page_id: int) -> None:
        """Internal method to delete a row (used by transaction and rollback)"""
        page = self.buffer_pool.load_page(page_id)
        if row_id in page.rows:
            del page.rows[row_id]
            self.buffer_pool.mark_dirty(page_id)
            self.buffer_pool.release_page(page_id)
            
            # Remove from index
            self.index.delete_row_mapping(row_id)

    def update_row(self, row_id: int, row: tuple, page_id: int) -> None:
        """Update an existing row."""
        page = self.buffer_pool.load_page(page_id)
        page.rows[row_id] = row
        self.buffer_pool.mark_dirty(page_id)
        self.buffer_pool.release_page(page_id)
        print(f"Updated row {row_id} on page {page_id}")

    def _get_current_page_id_from_disk(self):
        # Check if current page exists and has space
        if self.current_page_id in self.disk.pages:
            current_page = self.disk.pages[self.current_page_id]
            if len(current_page.rows) < self.rows_per_page:
                return self.current_page_id
        
        current_page_id = 1
        # Current page is full or doesn't exist, create new page
        if len(self.disk.pages) > 0:
            current_page_id = max(self.disk.pages.keys()) + 1

        return current_page_id
    
    def _get_current_page_id_from_buffer_pool(self):
        # Check if current page exists and has space
        if self.current_page_id in self.buffer_pool.pages:
            current_page = self.buffer_pool.pages[self.current_page_id]
            if len(current_page.page.rows) < self.rows_per_page:
                return self.current_page_id
        
        current_page_id = 1
        # Current page is full or doesn't exist, create new page
        if len(self.buffer_pool.pages) > 0:
            current_page_id = max(self.buffer_pool.pages.keys()) + 1
        
        return current_page_id
        
    def allocate_page_for_row(self) -> int:
        """
        Determine which page to insert a new row into.
        Strategy: Fill current page until full, then create new page.
        """
        self.current_page_id = max(self._get_current_page_id_from_disk(), self._get_current_page_id_from_buffer_pool())
        return self.current_page_id
    
    def checkpoint(self) -> None:
        """Clean checkpoint: flush all dirty pages and save to disk."""
        print("Shutting down engine...")
        self.buffer_pool.mark_clean_and_flush()
        self.disk.dump_to_json("disk.json")
        self.index.dump_to_json("index.json")
        print("✓ All dirty pages flushed")
        print("✓ Data saved to disk.json and index.json")