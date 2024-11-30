docker network create --driver bridge geo_query_network
docker build -t aggregator-node-image -f Dockerfile.aggregator .
docker build -t worker-node-image -f Dockerfile.worker .