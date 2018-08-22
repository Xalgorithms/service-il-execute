#!/bin/bash
echo "publishing $1 as latest"
docker tag xalgorithms/service-il-execute:$1 xalgorithms/service-il-execute:latest-development
docker push xalgorithms/service-il-execute:latest-development
