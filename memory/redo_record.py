from pydantic import BaseModel


class RedoLogRecordModel(BaseModel):
    lsn: int
    txid: int
    action: str
    data: dict
    page_id: int

class RedoRecord:
    def __init__(self):
        self.records : list[RedoLogRecordModel] = []
        self.flushed_lsn = 0
        self.redo_lsns = []

    def append(self, record: RedoLogRecordModel) -> None:
        self.records.append(record)
    
    def clear(self) -> None:
        self.records = []
    
    def flush(self) -> None:
        self.flushed_lsn = self.records[-1].lsn
    
    def dump_to_json(self, filename="redo_log.json") -> None:
        import json
        with open(filename, "w") as f:
            json.dump([record.dict() for record in self.records], f, indent=4)