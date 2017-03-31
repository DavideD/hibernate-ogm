#!/bin/bash
HTTP_NEO4J_PORT=${NEO4J_PORT:-7474}
BOLT_NEO4J_PORT=${BOLT_NEO4J_PORT:-7474}
echo "Starting neo4j container on port $NEO4J_PORT"

docker run -d --name Neo4j -p $HTTP_NEO4J_PORT:7474 -p 7373 -p $BOLT_NEO4J_PORT:7687 neo4j

