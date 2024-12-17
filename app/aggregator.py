from flask import Flask, request, jsonify, make_response, Response
from lib.globals import *
from lib.database import *
import os
import subprocess
import requests
import configparser
import json

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024
aggregator = None
db = None


mount_point = None
workers = []
worker_ids = []
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

# Sets up the tables to be partitioned evenly for default and leader-follower
def setup_partitions(messages: list[InitializationMessage], nodes: list[str], node_ids: list[str], message_iterator: list[str], distributed_iterator: list[str]) -> None:
    # Begin splitting on all valid partitioned tables
    for table in aggregator.partition:
        # To save time on initialization, we will split the table files into smaller files
        file_path = f"{aggregator.mount_point}/{table}.tbl"
        
        # Read number of lines in the file
        with open(file_path, 'r') as file:
            total_lines = sum(1 for _ in file)

        # Calculate the number of lines each node should process
        lines_per_node = total_lines // len(nodes)
        # Handle cases where the lines are not evenly divisible
        remainder = total_lines % len(nodes)

        # Define files for each node
        node_file_paths = {id: f'{aggregator.mount_point}/worker_{id}_{table}.tbl' for id in node_ids}
        for id in node_file_paths:
            open(node_file_paths[id], 'w').close()
        
        # Begin splitting process
        with open(file_path, 'r') as file:
            lines = file.readlines()
            
            start_line = 0
            for pos in range(len(nodes)):
                end_line = start_line + lines_per_node + (remainder if pos == len(nodes) - 1 else 0)
                node_file = node_file_paths[node_ids[pos]]

                # Write the assigned lines to the node's file
                with open(node_file, 'w') as node_file:
                    for line_num in range(start_line, end_line):
                        node_file.write(lines[line_num])
                
                start_line = end_line
        
        # Add partition-ed table file to be inserted
        for id in message_iterator:
            messages[id].insertion_tables.append(node_file_paths[id])

        print(f"Data has been split into {len(nodes)} files: {node_file_paths}")

    # Add non-partitioned tables
    if aggregator.mode == AggregatorMode.DISTRIBUTED:      
        for id in distributed_iterator:
            for table in aggregator.non_partition:
                messages[id].insertion_tables.append(f'{aggregator.mount_point}/{table}.tbl')
    
    # Since queries will be processed locally, we will insert non-partitioned tables
    if aggregator.mode == AggregatorMode.LOCAL:
        file = open(f'{mount_point}/load.sql', 'w')
        for table in aggregator.non_partition:
            file.write(f"\copy {table} FROM '{aggregator.mount_point}/{table}.tbl' DELIMITER '|' CSV;\n")
        file.close()
        subprocess.run([f"cd {aggregator.mount_point} && psql -U {db.user} -d {db.name} -f load.sql"], check=True, shell=True)

"""
Receives initialization configurations (program determined)
------------------------------------------------------------------------------------------
@arch: DEFAULT|FOLLOWER
@mode: LOCAL|DISTRIBUTED
@sample_query: SELECT ....
@number_query: 14
------------------------------------------------------------------------------------------
"""
@app.route('/receive_smart_init', methods=['POST'])
def receive_smart_init() -> Response:
    global aggregator

    # Already initialized, skip this process and let manager know
    if aggregator.initialized:
        return make_response("Initialized Already", 201)
    
    # Initialize the aggregator with setup conditions
    aggregator.arch = AggregatorArchitecture(request.json.get('arch'))
    aggregator.mode = AggregatorMode(request.json.get('mode'))
    sample_query = request.json.get('sample_query')
    number_query = request.json.get('number_query')
    aggregator.partition = DEFAULT_SMART_PARTITION
    aggregator.non_partition = DEFAULT_SMART_NON_PARTITION

    # Parse the tables from the sample SQL query
    tables = extract_tables(sqlparse.parse(sample_query)[0])
    # Separate tables into main query and subquery (inner 'FROM')
    main_query_tables = [t for t in tables if not t['subquery']]
    subquery_tables = [t for t in tables if t['subquery']]
    print(len(main_query_tables), len(subquery_tables))
    print(main_query_tables, subquery_tables)

    # Setup messages to be broadcasted to all worker nodes
    messages = {id: InitializationMessage(WorkerType.WORKER) for id in aggregator.worker_ids}

    # Only 1 table in usage (split on all tables)
    if len(main_query_tables) == 1 and len(subquery_tables) == 0:
        aggregator.partition = DEFAULT_ALL_TABLES
        aggregator.non_partition = []
    
    # Only 2 tables in usage (split on all tables but manually split on the WHERE key clause)
    if len(main_query_tables) == 2 and len(subquery_tables) == 0:
        # Handle query 14 (build new lineitem and part tables)
        if number_query == 14:
            if aggregator.arch == AggregatorArchitecture.DEFAULT:
                # Bucket all the parts by their p_partkey
                part_file_path = f"{mount_point}/part.tbl"
                part_buckets = dict()
                with open(part_file_path, 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        values = line.split('|')
                        p_partkey = int(values[0])
                        part_buckets[p_partkey] = line

                total_lines = len(part_buckets)

                # Calculate the number of lines each worker should process
                lines_per_worker = total_lines // len(aggregator.workers)

                # Generate part and lineitem files
                worker_file_paths_part = {id: f'{mount_point}/worker_{id}_part.tbl' for id in aggregator.worker_ids}
                for id in worker_file_paths_part:
                    open(worker_file_paths_part[id], 'w').close()
                worker_file_paths_lineitem = {id: f'{mount_point}/worker_{id}_lineitem.tbl' for id in aggregator.worker_ids}
                for id in worker_file_paths_lineitem:
                    open(worker_file_paths_part[id], 'w').close()
                
                # Temporary buffers to batch the writes for each worker
                lineitem_buffers = collections.defaultdict(list)
                part_buffers = collections.defaultdict(list)
                seen = set()

                # Split the lineitems and parts
                lineitem_file_path = f"{mount_point}/lineitem.tbl"
                with open(lineitem_file_path, 'r') as file:
                    for line in file:
                        l_partkey = int(line.split('|')[1])
                        worker_id = (l_partkey // lines_per_worker) % len(aggregator.worker_ids)
                        # Add lines to buffers
                        lineitem_buffers[str(worker_id + 1)].append(line)
                        if l_partkey not in seen:
                            part_buffers[str(worker_id + 1)].append(part_buckets[l_partkey])
                            seen.add(l_partkey)

                # Write all the batched data to the corresponding worker files at once
                for worker_id in aggregator.worker_ids:
                    with open(worker_file_paths_lineitem[worker_id], 'a') as f:
                        f.writelines(lineitem_buffers[worker_id])  # Write all lines at once
                    with open(worker_file_paths_part[worker_id], 'a') as f:
                        f.writelines(part_buffers[worker_id])  # Write all lines at once

                # Add partition-ed table file to be inserted
                for id in messages:
                    messages[id].insertion_tables.append(worker_file_paths_lineitem[id])
                    messages[id].insertion_tables.append(worker_file_paths_part[id])

                print(f"Data has been split into {len(aggregator.workers)} files: {worker_file_paths_part}, {worker_file_paths_lineitem}")
                

        # Handle query 12 (build new part and partsupp tables)
        if number_query == 12:
            pass
    
    # Only 3 tables in usage (split on all tables but manually split on the WHERE key clause)
    if len(main_query_tables) == 2 and len(subquery_tables) == 0:
        if number_query == 16:
            pass

"""
Receives initialization configurations
------------------------------------------------------------------------------------------
@partition (tables to be split): ["lineitem"]
@non_partition (tables to not be split): ["customer", "nation", "orders", "part", "partsupp", "region", "supplier"]
@arch: DEFAULT|FOLLOWER
@mode: LOCAL|DISTRIBUTED
------------------------------------------------------------------------------------------
"""
@app.route('/receive_init', methods=['POST'])
def receive_init() -> Response:
    global aggregator

    # Already initialized, skip this process and let manager know
    if aggregator.initialized:
        return make_response("Initialized Already", 201)

    # Initialize the aggregator with setup conditions
    aggregator.partition = request.json.get('partition')
    aggregator.non_partition = request.json.get('non_partition')
    aggregator.arch = AggregatorArchitecture(request.json.get('arch'))
    aggregator.mode = AggregatorMode(request.json.get('mode'))

    # Setup messages to be broadcasted to all worker nodes
    messages = {id: InitializationMessage(WorkerType.WORKER) for id in aggregator.worker_ids}

    # Follower represents Leader-Follower specialization
    # Early Configuration, clusters will be setup with 1 Leader and 2 Followers 
    # Only power of 3 node counts are supported
    if aggregator.arch == AggregatorArchitecture.FOLLOWER:
        if len(aggregator.workers) % 3 == 0:
            # Get all the ids that will correlate to the leaders
            for i in range(1, len(aggregator.worker_ids) - 1, 3):
                # Begin tracking leader
                aggregator.leader_ids.append(aggregator.worker_ids[i])
                aggregator.leaders.append(aggregator.workers[i])
                messages[aggregator.worker_ids[i]].worker_type = WorkerType.LEADER
                messages[aggregator.worker_ids[i]].follower_addresses.extend([aggregator.workers[i - 1], aggregator.workers[i + 1]])

                # Begin tracking followers
                aggregator.follower_ids.extend([aggregator.worker_ids[i - 1], aggregator.worker_ids[i + 1]])
                aggregator.followers.extend([aggregator.workers[i - 1], aggregator.workers[i + 1]]) 

                # Assign address to the two followers
                messages[aggregator.worker_ids[i - 1]].leader_address = aggregator.workers[i]
                messages[aggregator.worker_ids[i - 1]].worker_type = WorkerType.FOLLOWER
                messages[aggregator.worker_ids[i + 1]].leader_address = aggregator.workers[i]
                messages[aggregator.worker_ids[i + 1]].worker_type = WorkerType.FOLLOWER

            # Setups partitions for leaders and followers
            setup_partitions(messages, aggregator.followers, aggregator.follower_ids, aggregator.follower_ids, aggregator.leader_ids)
        else:
            aggregator.arch = AggregatorArchitecture.DEFAULT

    # Default is traditional multiple worker nodes with no specialization
    if aggregator.arch == AggregatorArchitecture.DEFAULT:
        setup_partitions(messages, aggregator.workers, aggregator.worker_ids, messages, messages)

    print(f"Messages: {messages}")

    # Send out initialization commands to all workers
    for worker in messages:
        endpoint = aggregator.workers[int(worker) - 1] + "/receive_init"
        tables = {path.split('/')[-1].replace('.tbl', '').split('_')[-1]: path for path in messages[worker].insertion_tables}
        payload = {
            "worker_type": messages[worker].worker_type.value,
            "files": tables,
            "leader_address": messages[worker].leader_address,
            "follower_addresses": messages[worker].follower_addresses
        }
        response = requests.post(endpoint, json=payload)

        if response.status_code != 200:
            print("Issue sending request to worker")
    
    aggregator.initialized = True
    return make_response("Success", 200)


@app.route('/send_task', methods=['POST'])
def send_task():
    query = request.json.get('query')
    tables = request.json.get('tables')
    query_id = request.json.get('query_id')
    results = {}

    # Handles default architecture
    # LOCAL AND DEFAULT
    # DISTRIBUTED AND DEFAULT
    start_time = time.time()
    if aggregator.arch == AggregatorArchitecture.DEFAULT:
        for worker_url in aggregator.workers:
            response = None

            if aggregator.mode == AggregatorMode.LOCAL:
                response = requests.post(f"{worker_url}/process_data", 
                                json={
                                "tables": tables,
                                "agg_url": f"http://aggregator:5001/receive_data"
                                })

            if aggregator.mode == AggregatorMode.DISTRIBUTED:
                response = requests.post(f"{worker_url}/process_query", 
                                json={
                                "query": query,
                                "agg_url": f"http://aggregator:5001/receive_result",
                                "query_id": query_id,
                                "worker_id": aggregator.worker_ids[aggregator.workers.index(worker_url)]
                                })
            
            if response.status_code != 200:
                print("Issue sending request to worker")
    
    # Handles leader-follower architecture
    # LOCAL AND FOLLOWER
    # DISTRIBUTED AND FOLLOWER
    if aggregator.arch == AggregatorArchitecture.FOLLOWER:
        for leader_url in aggregator.leaders:
            response = None

            if aggregator.mode == AggregatorMode.LOCAL:
                response = requests.post(f"{leader_url}/leader_data", 
                                json={
                                "tables": tables,
                                "agg_url": f"http://aggregator:5001/receive_data"
                                })

            if aggregator.mode == AggregatorMode.DISTRIBUTED:
                response = requests.post(f"{leader_url}/leader_results", 
                                json={
                                "query": query,
                                "agg_url": f"http://aggregator:5001/receive_result",
                                "query_id": query_id,
                                "worker_id": aggregator.worker_ids[aggregator.workers.index(leader_url)]
                                })
            
            if response.status_code != 200:
                print("Issue sending request to leader")
    
    end_time = time.time()
    # Write network latency to json file with query_id
    results["network_latency"] = end_time - start_time
    with open(f"query-results/{query_id}_network_latency.json", "w") as f:
        f.write(json.dumps(results))

    if aggregator.mode == AggregatorMode.LOCAL:
        # Run the query on the aggregator
        start_time = time.time()
        results = db.execute_query(query)
        end_time = time.time()
        results.append({"query_time": end_time - start_time})
        
        # Write results to json file with query_id
        with open(f"query-results/{query_id}_aggregator.json", "w") as f:
            f.write(json.dumps(results))
        
    return jsonify(results)

@app.route('/receive_result', methods=['POST'])
def receive_result():
    data = request.json
    results = data["results"]
    query_id = data["query_id"]
    worker_id = data["worker_id"]
    
    # write results to json file with query_id
    with open(f"query-results/{query_id}_worker_{worker_id}.json", "w") as f:
        f.write(json.dumps(results))
    
    return make_response("Success", 200)

@app.route('/receive_data', methods=['POST'])
def receive_data():
    table = request.json.get('name')
    rows = request.json.get('rows')
    # Insert data into database
    db.insert_rows(Table(table, rows))
    return make_response("Success", 200)

def send_init():
    worker_id = 0
    for table in tables:
        if table == "lineitem":
            for batch in db.fetch_all(table):
                response = requests.post(
                    f"{workers[worker_id]}/init", 
                    json={"name": table, "rows": batch}
                )
                print(response)
                if response.status_code != 200:
                    print("Error sending init to worker")
                worker_id = (worker_id + 1) % len(workers)
        else:
            for worker_url in workers:
                for batch in db.fetch_all(table):
                    response = requests.post(
                        f"{worker_url}/init", 
                        json={"name": table, "rows": batch}
                    )
                    if response.status_code != 200:
                        print("Error sending init to worker")
                        
    # Remove rows from database
    db.delete_rows("lineitem")

def init_aggregator() -> Database:
    global aggregator, db, mount_point, workers, worker_ids
    
    # Get initial configurations
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', 5432)
    name = os.getenv('DB_NAME', 'postgres')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    schema = os.getenv('DB_SCHEMA', 'schema.sql')

    # Determines how make workers were generated with system-nodes.sh
    # References the config.ini file to figure this out
    config = configparser.ConfigParser()
    config.read('config.ini')
    number_of_workers = int(config['AGGREGATOR']['number_of_workers'])
    worker_port = int(config['AGGREGATOR']['port'])
    mount_point = config["SHARED"]["mount_point"]
    workers, worker_ids = [], []
    for w in range(1, number_of_workers + 1):
        workers.append(DEFAULT_WORKER_NAME + f"{w}:{worker_port}")
        worker_ids.append(str(w))

    # Compile dbgen using subprocess
    subprocess.run(["cd TPC-H/dbgen && make"], check=True, shell=True)
    
    # Create .tbl files "./dbgen -s 1"
    subprocess.run(["cd TPC-H/dbgen && ./dbgen -s 1"], check=True, shell=True)

    # After generation, move all the .tbl files to shared mount point
    subprocess.run([f"mv TPC-H/dbgen/*.tbl {mount_point}"], check=True, shell=True)

    # Initialize aggregator with basic init information
    aggregator = Aggregator(mount_point, workers, worker_ids)

    # Setup Database
    db = Database(host, port, name, user, password, schema)
    
    # Data is in load.sql
    # subprocess.run([f"mv load.sql {mount_point}"], check=True, shell=True)
    # subprocess.run([f"cd {mount_point} && psql -U {db.user} -d {db.name} -f load.sql"], check=True, shell=True)
    
def main():
    init_aggregator()
    app.run(host='0.0.0.0', port=5001)

if __name__ == '__main__':
    main()