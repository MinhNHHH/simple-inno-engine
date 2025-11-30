class LockTable:
    def __init__(self):
        self.locks = {}  # page_id -> {txid, type}
