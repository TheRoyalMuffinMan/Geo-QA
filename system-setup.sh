docker build -t primary-node-image -f Dockerfile.primary .
docker build -t worker-node-image -f Dockerfile.worker .
docker run --network geo_query_network --name primary_node -p 5000:5000 -d primary-node-image
docker run --network geo_query_network --name worker_node_1 -d worker-node-image
docker run --network geo_query_network --name worker_node_2 -d worker-node-image