#!/usr/bin/env bash

set -E

rm -rf ~/.m2/repository/org/corfudb

./mvnw clean install -DskipTests=true -DcreateChecksum=true -Dmaven.javadoc.skip=true -B -V -T 1C

./support/corfu_artifacts_uploader.sh $1