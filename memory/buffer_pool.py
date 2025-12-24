from memory.disks import Disk
from memory.pages import Page
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
    def __init__(self, capacity: int, disk: Disk):
        self.capacity = capacity
        self.pages : dict[int, PageNode] = {}  # page_id -> Page
        self.disk = disk
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
            self.mark_dirty(page_id)
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
        if lru.page.dirty:
            self.disk.write_page(lru.page)
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
        for page_id in self.pages:
            page = self.pages[page_id].page
            if page.dirty:
                page.dirty = False
                self.disk.write_page(page)