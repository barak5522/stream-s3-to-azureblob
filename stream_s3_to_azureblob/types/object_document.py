from enum import Enum

class status(Enum):
    pending = 'pending'
    downloaded = 'downloaded'
    failed = 'failed'

class object_type(Enum):
    rawiq = 'rawiq'
    rfgeo = 'rfgeo'

class object_document:
    key: str
    type: object_type
    pass_group_id: str
    retries: int
    status: status