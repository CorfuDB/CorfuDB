#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    echo -e "Publishing docker image..."

    docker build -t corfu .
    docker tag corfu corfudb/corfu:latest
    docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
    docker push corfudb/corfu

    echo -e "Docker image pushed."
fi