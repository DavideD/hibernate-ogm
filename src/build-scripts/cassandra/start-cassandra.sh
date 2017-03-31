#!/bin/bash
CASSANDRA_PORT=${CASSANDRA_PORT:-9042}
echo "Starting Cassandra container on port $CASSANDRA_PORT"

docker run -d --name Cassandra -p $CASSANDRA_PORT:9042 cassandra:3.7

