class Page:
    def __init__(self, rows: list[tuple], page_id: int, page_lsn: int):
        self.page_id = page_id
        self.rows : dict[int, tuple] = {i: row for i, row in enumerate[tuple](rows)}  # row_id -> tuple
        self.page_lsn = page_lsn
        self.dirty = False
        self.pinned = False
        self.pin_count = 0