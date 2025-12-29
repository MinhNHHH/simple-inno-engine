from memory.disks import Disk
from memory.pages import Page
from memory.double_write_buffer import DoublewriteBuffer

import threading

class PageNode:
    def __init__(self, page: Page):
        self.page = page
        self.prev = None
        self.next = None

class BufferPool:
    """
    Buffer Pool is a cache for pages. It is used to store pages that are being used by the database.
    It is a LRU cache, so the least recently used page is evicted when the cache is full.

    Buffer Pool work:
        - When a page is loaded from the disk, it is added to the buffer pool.
        - When a page is modified, it is marked as dirty.
        - When the buffer pool is full, the least recently used page is evicted.
        - When a page is pinned, it is not evicted. (pin_count > 0)
        - When a page is evicted, it is written to the disk.
        - When a page is written to the disk, it is marked as clean.
        - When a page is read from the disk, it is marked as clean.
    """
    def __init__(self, capacity: int, disk: Disk, double_write_buffer: DoublewriteBuffer):
        self.capacity = capacity
        self.pages : dict[int, PageNode] = {}  # page_id -> Page
        self.disk = disk
        self.double_write_buffer = double_write_buffer
        self.head = PageNode(page=None)
        self.tail = PageNode(page=None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.lock = threading.Lock()
    
    def load_page(self, page_id: int) -> Page:
        with self.lock:
            print(f"Loading page {page_id} from buffer pool")
            if page_id in self.pages:
                self.pages[page_id].page.pin_count += 1
                self._move_to_head(self.pages[page_id])
                return self.pages[page_id].page
            page = self.disk.get_page(page_id)
            self.add_page_to_memory(page)
            page.pin_count += 1
            print(f"Pages in buffer pool: {self.pages}")
            return page
    
    def add_page_to_memory(self, page: Page) -> None:
        if page.page_id in self.pages:
            return
        node = PageNode(page=page)
        if len(self.pages) == self.capacity:
            self._evict_page()
        self._add_node(node)
        self.pages[page.page_id] = node

    def _move_to_head(self, node: PageNode) -> None:
        self._remove_node(node)
        self._add_node(node)
        return
    
    def _add_node(self, node: PageNode) -> None:
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def _remove_node(self, node: PageNode) -> None:
        node.prev.next = node.next
        node.next.prev = node.prev
    
    def _evict_page(self) -> None:
        lru = self.tail.prev
        # find the least recently used page that is not pinned
        while lru != self.head:
            if lru.page.pin_count == 0: 
                break
            lru = lru.prev
        
        # Check if we found an evictable page
        if lru == self.head:
            raise Exception("All pages are pinned and cannot be evicted")
        
        # At this point, lru.page.pin_count must be 0
        # If page is dirty, we need to write it to disk using doublewrite buffer protocol
        # This is the CRITICAL path for data durability and crash safety
        if lru.page.dirty:
            # Write to doublewrite buffer
            self.double_write_buffer.add_page(lru.page)
            self.double_write_buffer.fsync()
            
            # Write to actual disk location
            self.disk.write_page(lru.page)
            
            # Clear DWB entry after successful write
            self.double_write_buffer.clear()
        self._remove_node(lru)
        del self.pages[lru.page.page_id]

    def release_page(self, page_id: int) -> None:
        if page_id not in self.pages:
            raise Exception("Page not found in buffer pool")
        page = self.pages[page_id].page
        if page.pin_count <= 0:
            raise Exception("Unbalanced pin/unpin")
        page.pin_count -= 1
        if page.pin_count == 0:
            page.pinned = False
    
    def mark_dirty(self, page_id: int) -> None:
        if page_id not in self.pages:
            raise Exception("Page not found in buffer pool")
        self.pages[page_id].page.dirty = True
    
    def mark_clean_and_flush(self) -> None:
        """
        Flush all dirty pages using double-write buffer protocol
        This is the CRITICAL path for data durability and crash safety
        """
        # Collect all dirty pages and add to DWB
        dirty_pages = []
        with self.lock:
            for page_id in self.pages:
                page = self.pages[page_id].page
                if page.dirty:
                    dirty_pages.append(page)
                    self.double_write_buffer.add_page(page)
        
        if not dirty_pages:
            return 
        
        # Flush DWB to disk (sequential write - creates backup)
        print(f"[FLUSH] Writing {len(dirty_pages)} dirty pages via DWB")
        self.double_write_buffer.fsync()
        
        # Write pages to their ACTUAL disk locations (permanent storage)
        # THIS IS CRITICAL! DWB is just temporary backup, not the actual storage!
        for page in dirty_pages:
            page.dirty = False
            self.disk.write_page(page)
        
        self.double_write_buffer.clear()
        print(f"[FLUSH] Successfully flushed {len(dirty_pages)} pages")