class UndoLog:
    def __init__(self):
        self.records = {}  # txid -> stack of undo records
