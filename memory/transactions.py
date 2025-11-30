class TransactionTableEntry:
    def __init__(self, txid, status, last_lsn):
        self.txid = txid
        self.status = status
        self.last_lsn = last_lsn

class TransactionTable:
    def __init__(self):
        self.active = {}  # txid -> TransactionTableEntry
