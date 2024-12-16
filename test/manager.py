from enum import Enum
import argparse
import requests
import sys

TPC_H_TABLES = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
API_URL = "http://localhost:5001"
INITIALIZATION_ENDPOINT = "/receive_init"
TASK_ENDPOINT = "/send_task"

parser = argparse.ArgumentParser(description="Manager program that controls nodes setup and queries")
parser.add_argument(
    '-p', '--partition', nargs='+', 
    help=('<Required> List of tables in TPC-H set that will be partitioned, '
          'possible tables: ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]'),
    required=True
)
parser.add_argument('-a', '--arch', type=int, help='<Required> Nodes architecture (0:DEFAULT|1:FOLLOWER)', required=True)
parser.add_argument('-m', '--mode', type=int, help='<Required> Nodes mode (0:LOCAL|1:DISTRIBUTED)', required=True)

class AggregatorMode(Enum):
    LOCAL = 0
    DISTRIBUTED = 1
    NOT_SET = 2

class AggregatorArchitecture(Enum):
    DEFAULT = 0
    FOLLOWER = 1
    NOT_SET = 2

# Processes initialization of the system
def initialize_system(api_url: str, endpoint: str, partition: list[str], non_partition: list[str], arch: AggregatorArchitecture, mode: AggregatorMode) -> None:
    # Prepare JSON payload
    payload = {
        "partition": partition,
        "non_partition": non_partition,
        "arch": arch.value,
        "mode": mode.value
    }

    # Send POST request
    headers = {"Content-Type": "application/json"}
    response = requests.post(api_url + endpoint, json=payload, headers=headers)
    
    # Check for response status
    if response.status_code != 200:
        if response.status_code == 201:
            print("System already initialized")
        else:
            sys.exit("Error with initialization")
    
    return


def send_sql_query(api_url: str, query_type: str, tables: str, sql_query: str, query_id: str) -> dict:
    try:
        # Prepare JSON payload
        payload = {
            "type": query_type,
            "tables": tables,
            "query": sql_query,
            "query_id": query_id
        }

        # Send POST request
        headers = {"Content-Type": "application/json"}
        response = requests.post(api_url, json=payload, headers=headers)
        
        # Check for response status
        response.raise_for_status()
        
        # Parse JSON response
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return {"error": str(e)}

# Example usage
# api_url = "http://localhost:5001/send_task"
# query_type = "ResponseType.QUERY"
# tables = ["lineitem"]
# query_id = "1"
# # Read SQL query from file queries/1.sql
# sql_query = open("test_queries/1.sql").read()

# response = send_sql_query(api_url, query_type, tables, sql_query, query_id)
# print(response)

# # Long running: 2, 17, 20
# # Queries that work: 1, 2, 3, 5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19
# # Queries that don't work: 4, 13, 17, 20

# # Use docker cp to retrieve the results from the container
# # docker cp <container_id>:/app/query-results ./
# subprocess.run(["docker", "cp", "aggregator:/app/query-results", "./"])

# Verify the arguments passed in
def verify_arguments(args):
    for part in args.partition:
        if part not in TPC_H_TABLES:
            sys.exit("Passed invalid table to split")
    
    if args.arch != 0 and args.arch != 1:
        sys.exit("Passed invalid architecture")
    
    if args.mode != 0 and args.mode != 1:
        sys.exit("Passed invalid mode")

def main() -> None:
    args = parser.parse_args()
    verify_arguments(args)
    partition = args.partition
    non_partition = [table for table in TPC_H_TABLES if table not in partition]
    arch = AggregatorArchitecture(args.arch)
    mode = AggregatorMode(args.mode)

    # Initialize the system with command line arguments
    initialize_system(API_URL, INITIALIZATION_ENDPOINT, partition, non_partition, arch, mode)


if __name__ == "__main__":
    main()