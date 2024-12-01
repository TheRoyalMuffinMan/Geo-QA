from enum import Enum

POSTGRESQL_CONFIG_FILE = "/etc/postgresql/15/main/pg_hba.conf"

class AggregatorMode(Enum):
    LOCAL = 0
    DISTRIBUTED = 1

class ResponseType(Enum):
    RESULTS = 0
    DATA = 1