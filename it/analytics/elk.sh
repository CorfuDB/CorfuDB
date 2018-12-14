#!/usr/bin/env bash

set -e

ELK_DIR=generated/elk

rm -rf ${ELK_DIR}
git clone git@github.com:deviantony/docker-elk.git ${ELK_DIR}

docker kill filebeat
docker rm filebeat

cd ${ELK_DIR} && docker-compose down && docker-compose up -d

sleep 60

cd ../../filebeat && docker run -d --name=filebeat --user=root --network=elk_elk \
    --volume="$(pwd)/filebeat.docker.yml:/usr/share/filebeat/filebeat.yml:ro" \
    --volume="/var/lib/docker/containers:/var/lib/docker/containers:ro" \
    --volume="/var/run/docker.sock:/var/run/docker.sock:ro" \
    docker.elastic.co/beats/filebeat:6.5.3 \
    filebeat -e -strict.perms=false
