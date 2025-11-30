class RedoRecord:
    def __init__(self, lsn, txid, action, data):
        self.lsn = lsn
        self.txid = txid
        self.action = action
        self.data = data
