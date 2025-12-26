from pydantic import BaseModel
from typing import Optional

class UndoRecordModel(BaseModel):
    """Undo record for rollback operations"""
    row_id: int
    page_id: int
    old_value: Optional[tuple] = None  # None means INSERT operation
    operation: str  # "INSERT", "UPDATE", "DELETE"


class UndoRecord:
    def __init__(self):
        self.records: list[UndoRecordModel] = []

    def append(self, record: UndoRecordModel) -> None:
        self.records.append(record)
    
    def clear(self) -> None:
        self.records = []
    
    def dump_to_json(self, filename="undo_log.json") -> None:
        import json
        with open(filename, "w") as f:
            json.dump([record.dict() for record in self.records], f, indent=4)
