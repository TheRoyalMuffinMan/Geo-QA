#!/bin/bash

shared_volume_name="shared_volume"

# Stop and remove the aggregator container
if docker ps -a --filter "name=aggregator" --format "{{.Names}}" | grep -q "aggregator"; then
    echo "Stopping container: aggregator"
    docker stop aggregator
    echo "Removing container: aggregator"
    docker rm aggregator
else
    echo "No aggregator container found."
fi

# Stop and remove all worker containers
echo "Stopping and removing worker containers..."
for container in $(docker ps -a --filter "name=worker_node_" --format "{{.Names}}"); do
    echo "Stopping container: $container"
    docker stop "$container"
    echo "Removing container: $container"
    docker rm "$container"
done

# Check if the shared Docker volume exists
if docker volume ls --filter "name=$shared_volume_name" --format "{{.Name}}" | grep -q "$shared_volume_name"; then
    echo "Removing shared volume: $shared_volume_name"
    docker volume rm "$shared_volume_name"
else
    echo "No shared volume found with name: $shared_volume_name"
fi

echo "All specified containers have been stopped and removed."
