#!/bin/bash

echo "Stopping all running containers..."
docker stop $(docker ps -q)

echo "Removing all containers..."
docker rm $(docker ps -a -q)
