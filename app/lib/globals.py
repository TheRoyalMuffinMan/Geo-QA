from enum import Enum
import collections

Table = collections.namedtuple("Table", ["name", "rows"])

POSTGRESQL_CONFIG_FILE = "/etc/postgresql/15/main/pg_hba.conf"
DEFAULT_WORKER_NAME = "http://worker_node_"

class AggregatorMode(Enum):
    LOCAL = 0
    DISTRIBUTED = 1

class ResponseType(Enum):
    RESULTS = 0
    DATA = 1