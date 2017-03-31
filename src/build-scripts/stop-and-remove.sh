#!/bin/bash
NAME=$1
echo "Stopping and removing $NAME container ..."
docker stop "$NAME"
docker rm -f "$NAME"
