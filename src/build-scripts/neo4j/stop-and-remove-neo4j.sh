#!/bin/bash
echo "Stopping and removing Neo4j container ..."

docker stop Neo4j
echo "Neo4j stopped"

docker rm -f Neo4j
echo "Neo4j removed"
