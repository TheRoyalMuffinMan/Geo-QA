#!/bin/bash

# Path to the configuration file
CONFIG_FILE="app/config.ini"

# Function to get a value from a specific section
get_config_value() {
    local section=$1
    local key=$2
    local value=$(awk -F '=' -v section="[$section]" -v key="$key" '
        $0 == section { in_section=1; next }
        in_section && $1 == key { print $2; exit }
        /^\[/{ in_section=0 }
    ' "$CONFIG_FILE" | tr -d ' ')
    echo "$value"
}

# Acquire number of workers and port for all nodes
workers=$(get_config_value "AGGREGATOR" "number_of_workers")
port=$(get_config_value "AGGREGATOR" "port")
shared_volume_path=$(get_config_value "SHARED" "mount_point")
volume_name="shared_volume"

# Generate the shared volume that all the nodes will have access to
docker volume create "$volume_name"

# Iterate through the number of workers and initialize worker nodes (within docker)
for ((i=1; i<=workers; i++)); do
    container_name="worker_node_$i"
    docker run \
        --network geo_query_network \
        --name "$container_name" \
        -v "$volume_name:$shared_volume_path" \
        -d worker-node-image
    echo "Started container: $container_name with shared volume mounted at $shared_volume_path"
done

# Initialize aggregator node at the end once all worker nodes have been initialized (within docker)
docker run \
    --network geo_query_network \
    --name aggregator -p "$port:$port" \
    -v "$volume_name:$shared_volume_path" \
    -d aggregator-node-image
echo "Started container: aggregator with shared volume mounted at $shared_volume_path"