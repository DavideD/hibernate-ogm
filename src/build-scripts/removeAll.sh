#!bin/bash
for DB_NAME in "CouchDB" "Neo4j" "Redis" "Cassandra"
do
  sh stop-and-remove.sh $DB_NAME
done
