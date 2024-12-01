docker run --network geo_query_network --name aggregator -p 5000:5000 -d aggregator-node-image
docker run --network geo_query_network --name worker_node_1 -d worker-node-image
docker run --network geo_query_network --name worker_node_2 -d worker-node-image