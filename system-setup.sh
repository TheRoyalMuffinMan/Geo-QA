#!/bin/bash

# Creates network bridge
docker network create --driver bridge geo_query_network

# Installs docker images to be used to create containers
docker build -t aggregator-node-image -f Dockerfile.aggregator .
docker build -t worker-node-image -f Dockerfile.worker .