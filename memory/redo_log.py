class RedoLog:
    def __init__(self):
        self.records = {}  # LSN -> log record
