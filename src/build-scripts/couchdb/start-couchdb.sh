#!/bin/bash
COUCHDB_PORT=${COUHDB_PORT:-5984}
echo "Starting docker container on port $COUCHDB_PORT"

docker run -d --name CouchDB -p $COUCHDB_PORT:5984 couchdb

