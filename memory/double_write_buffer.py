from memory.pages import Page
from memory.disks import Disk
import copy
from threading import Lock
import json

class DoublewriteBuffer:
    """
    Double Write Buffer - Prevents torn page writes    
    How it works:
    1. Pages are first written sequentially to the DWB area (this file)
    2. fsync() ensures DWB area is persisted to disk
    3. Then pages are written to their actual scattered locations
    4. If crash occurs during step 3, recovery uses complete DWB copy
    """
    def __init__(self, disk: Disk, dwb_file: str = "doublewrite_buffer.json"):
        self.disk = disk
        self.pages : dict[int, Page] = {}  # Staging area in memory
        self.lock: Lock = Lock()
        self.dwb_file = dwb_file  # Dedicated DWB storage on "disk"
        self.dwb_storage: dict[int, Page] = {}  # Simulates DWB disk area
    
    def add_page(self, page: Page) -> None:
        """
        Add a page to the doublewrite buffer staging area.
        This does NOT write to disk yet - just prepares the page.
        """
        with self.lock:
            self.pages[page.page_id] = copy.deepcopy(page)
    
    def fsync(self) -> None:
        """
        Flush all staged pages to the DWB AREA on disk (sequential write).
        
        This is the CRITICAL difference from the wrong implementation:
        - Writes to dedicated DWB sequential area (fast write)
        - Simulates sequential disk I/O
        In this implementation: We simulate it with a separate JSON file.
        """
        with self.lock:
            if not self.pages:
                return
            for page_id, page in self.pages.items():
                self.dwb_storage[page_id] = copy.deepcopy(page)            
            self._persist_dwb_to_disk()
            print(f"[DWB] Wrote {len(self.pages)} pages to DWB sequential area")
    
    def _persist_dwb_to_disk(self) -> None:
        """
        Persist the DWB area to disk.
        In real InnoDB: This is a sequential write + fsync.
        In our simulation: Write to a separate JSON file.
        """
        try:
            serializable = {}
            for page_id, page in self.dwb_storage.items():
                serializable[int(page_id)] = {
                    "page_id": page.page_id,
                    "rows": page.rows,
                    "page_lsn": page.page_lsn,
                    "dirty": page.dirty,
                }
            
            with open(self.dwb_file, "w") as f:
                json.dump(serializable, f, indent=2)
        except Exception as e:
            print(f"[DWB] Error persisting DWB: {e}")
    
    def recover_from_dwb(self, page_id: int) -> Page | None:
        """
        Recover a page from the DWB area (used during crash recovery).
        
        When to use:
        - After crash, if actual page is corrupted/torn
        - Check DWB for a complete copy
        - Restore from DWB to actual location
        """
        with self.lock:
            if page_id in self.dwb_storage:
                return copy.deepcopy(self.dwb_storage[page_id])
            return None
    
    def clear(self) -> None:
        """
        Clear the staging area after successful writes to actual locations.
        
        Important: This clears the in-memory staging area, but keeps
        the DWB disk area intact until next checkpoint (for crash recovery).
        """
        with self.lock:
            self.pages.clear()
    
    def clear_dwb_area(self) -> None:
        """
        Clear the entire DWB area (after successful checkpoint).
        Only call this after ALL pages have been safely written to 
        their actual locations.
        """
        with self.lock:
            self.dwb_storage.clear()
            try:
                with open(self.dwb_file, "w") as f:
                    json.dump({}, f)
            except Exception as e:
                print(f"[DWB] Error clearing DWB: {e}")
    
    def delete_page(self, page_id: int) -> None:
        with self.lock:
            if page_id not in self.pages:
                raise Exception(f"Page {page_id} not found in DWB staging")
            del self.pages[page_id]
    
    def get_stats(self) -> dict:
        with self.lock:
            return {
                "staged_pages": len(self.pages),
                "dwb_area_pages": len(self.dwb_storage),
                "dwb_file": self.dwb_file,
            }