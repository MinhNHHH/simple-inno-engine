from memory.pages import Page
import copy

class Disk:
    def __init__(self):
        self.pages : dict[int, Page] = {}  # page_id -> Page

    def get_page(self, page_id: int) -> Page:
        if page_id not in self.pages:
            raise Exception(f"Page {page_id} not found on disk")
        # Return a deep copy of the page to avoid modifying the original page
        return copy.deepcopy(self.pages[page_id])

    def write_page(self, page: Page) -> None:
        # the disk must store its own independent copy of the page, unaffected by later in-memory modifications.
        self.pages[page.page_id] = copy.deepcopy(page)
        return
        
    def delete_page(self, page_id: int) -> None:
        if page_id not in self.pages:
            raise Exception(f"Page {page_id} not found on disk")
        del self.pages[page_id]

    # Store all disk pages into a JSON file called 'disk.json'
    def dump_to_json(self, filename="disk.json"):
        import json
        def page_to_dict(page):
            d = {"page_id": page.page_id}
            for attr in dir(page):
                if attr.startswith("_") or attr in ("page_id", "pinned", "pin_count", "dirty"):
                    continue
                v = getattr(page, attr)
                if isinstance(v, (int, str, list, dict, float, bool, type(None))):
                    d[attr] = v
            d["pinned"] = getattr(page, "pinned", False)
            d["pin_count"] = getattr(page, "pin_count", 0)
            d["dirty"] = getattr(page, "dirty", False)
            return d

        serializable = {int(pid): page_to_dict(page) for pid, page in self.pages.items()}
        with open(filename, "w") as f:
            json.dump(serializable, f, indent=4)

    def load_from_json(self, filename="disk.json") -> None:
        import json
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            for pid, page_data in data.items():
                page_data["rows"] = [tuple(v) for v in page_data["rows"].values()]
                page = Page(rows={}, page_id=int(page_data["page_id"]), page_lsn=int(page_data["page_lsn"]))
                for attr, value in page_data.items():
                    if attr == "page_id":
                        continue
                    setattr(page, attr, value)
                page.rows = {int(row[0]): row for _, row in enumerate(page_data["rows"])}
                self.pages[int(pid)] = page
        except:
            return {}
    def get_current_page_id(self) -> int:
        if len(self.pages) == 0:
            return 1
        return max(self.pages.keys())