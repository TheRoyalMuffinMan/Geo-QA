import enum

class AggregatorMode(enum):
    LOCAL = 0
    DISTRIBUTED = 1

class ResponseType(enum):
    RESULTS = 0
    DATA = 1