docker network create --driver bridge geo_query_network
docker build -t aggregator-node-image -f Dockerfile.aggregator .
docker build -t worker-node-image -f Dockerfile.worker .
docker run --network geo_query_network --name aggregator -p 5000:5000 -d aggregator-node-image
docker run --network geo_query_network --name worker_node_1 -d worker-node-image
docker run --network geo_query_network --name worker_node_2 -d worker-node-image