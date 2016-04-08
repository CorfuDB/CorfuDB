#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
openssl aes-256-cbc -K $1 -iv $2 -in private.key.enc -out private.key -d
fi