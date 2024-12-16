from enum import Enum
import collections

Table = collections.namedtuple("Table", ["name", "rows"])

POSTGRESQL_CONFIG_FILE = "/etc/postgresql/15/main/pg_hba.conf"
DEFAULT_WORKER_NAME = "http://worker_node_"

class AggregatorMode(Enum):
    LOCAL = 0
    DISTRIBUTED = 1
    NOT_SET = 2

class AggregatorArchitecture(Enum):
    DEFAULT = 0
    FOLLOWER = 1
    NOT_SET = 2

class WorkerType(Enum):
    WORKER = 0
    FOLLOWER = 1
    LEADER = 2
    NOT_SET = 3

class ResponseType(Enum):
    RESULTS = 0
    DATA = 1
    QUERY = 2

class InitializationMessage:
    def __init__(
            self,
            worker_type: int,
            insertion_tables: list[str] = None,
            leader_address: str = None,
            follower_addresses: list[str] = None
    ) -> None:
        self.worker_type = worker_type
        self.insertion_tables = insertion_tables if insertion_tables is not None else []
        self.leader_address = leader_address if leader_address is not None else ""
        self.follower_addresses = follower_addresses if follower_addresses is not None else []
    
    def __repr__(self):
        return (f"InitializationMessage(insertion_tables={self.insertion_tables}, "
                f"leader_address='{self.leader_address}', "
                f"worker_type={self.worker_type}, "
                f"follower_addresses={self.follower_addresses})")

class Aggregator:
    def __init__(
            self,
            mount_point: str, 
            workers: list[str], 
            worker_ids: list[str], 
            partition: list[str] = None, 
            non_partition: list[str] = None, 
            mode: AggregatorMode = AggregatorMode.NOT_SET,
            arch: AggregatorArchitecture = AggregatorArchitecture.NOT_SET,
            initialized: bool = False,
            leader_ids: list[str] = None,
            leaders: list[str] = None,
            follower_ids: list[str] = None,
            followers: list[str] = None
    ) -> None:
        self.mount_point = mount_point
        self.workers = workers
        self.worker_ids = worker_ids
        self.partition = partition if partition is not None else []
        self.non_partition = non_partition if partition is not None else []
        self.mode = mode
        self.arch = arch
        self.initialized = initialized
        self.leader_ids = leader_ids if leader_ids is not None else []
        self.leaders = leaders if leaders is not None else []
        self.follower_ids = follower_ids if follower_ids is not None else []
        self.followers = followers if followers is not None else []

    def __repr__(self):
        return (
            f"Aggregator(\n"
            f"  mount_point={self.mount_point!r},\n"
            f"  workers={self.workers!r},\n"
            f"  worker_ids={self.worker_ids!r},\n"
            f"  partition={self.partition!r},\n"
            f"  non_partition={self.non_partition!r},\n"
            f"  mode={self.mode!r},\n"
            f"  arch={self.arch!r},\n"
            f"  initialized={self.initialized!r},\n"
            f"  leader_ids={self.leader_ids!r},\n"
            f"  leaders={self.leaders!r},\n"
            f"  follower_ids={self.follower_ids!r},\n"
            f"  followers={self.followers!r}\n"
            f")"
        )
    
class Worker:
    def __init__(
            self,
            mount_point: str = None,
            worker_type: WorkerType = WorkerType.NOT_SET,
            leader_address: str = None,
            follower_addresses: list[str] = None,
            partition_tables: list[str] = None
    ) -> None:
        self.mount_point = mount_point if mount_point is not None else ""
        self.worker_type = worker_type
        self.leader_address = leader_address if leader_address is not None else ""
        self.follower_addresses = follower_addresses if follower_addresses is not None else []
        self.partition_tables = partition_tables if partition_tables is not None else []
    

    def __repr__(self):
        return (f"Worker(mount_point='{self.mount_point}', "
                f"worker_type={self.worker_type}, "
                f"leader_address='{self.leader_address}'"
                f"follower_addresses={self.follower_addresses}"
                f"partition_tables={self.partition_tables}")