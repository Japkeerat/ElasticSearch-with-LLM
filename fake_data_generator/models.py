from dataclasses import dataclass, field
from typing import List
from datetime import datetime

@dataclass
class FieldChange:
    field: str
    old: str
    new: str

@dataclass
class AuditLog:
    log_id: str
    timestamp: datetime
    user: dict
    session: dict
    action: str
    entity: dict
    client_ip: str
