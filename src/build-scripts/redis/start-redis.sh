#!/bin/bash
echo "Starting Redis container on port 6379"

docker run -d --name Redis -p 66379 redis

