#!/usr/bin/env bash

set -e

./mvnw -pl generator --also-make -DskipTests -Dmaven.javadoc.skip=true -T 1C clean package

CORFU_VERSION=$(./mvnw -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)

cd generator

# Docker build
docker build --network=host \
  -t corfudb/generator:"${CORFU_VERSION}" \
  -t corfudb-universe/generator:"${CORFU_VERSION}" \
  .
