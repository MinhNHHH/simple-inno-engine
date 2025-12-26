class Page:
    def __init__(self, rows: list[tuple], page_id: int, page_lsn: int):
        self.page_id = page_id
        self.rows : dict[int, tuple] = {int(row[0]): row for _, row in enumerate(rows)}  # row_id -> tuple
        self.page_lsn = page_lsn
        self.dirty = False
        self.pinned = False
        self.pin_count = 0