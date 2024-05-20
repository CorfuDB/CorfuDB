#!/usr/bin/env bash

set -e

PROFILE=$1
BASE_IMAGE=$2
if [ -z "$PROFILE" ]; then
  echo "Usage: './infrastructure-docker-build.sh <profile>', available profiles: docker, compatibility"
  exit 1
fi

# Clean up project
./mvnw clean

# Build cmdlets
./mvnw -pl cmdlets,corfudb-tools,infrastructure \
  --also-make \
  -DskipTests \
  -Dmaven.javadoc.skip=true \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  -T 1C \
  package

# Copy corfu binaries into target directory
cp -r ./bin ./infrastructure/target/
cp -r ./corfu_scripts ./infrastructure/target/
cp -r ./scripts ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/logback.prod.xml ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/compactor-logback.prod.xml ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/corfu-compactor-config.yml ./infrastructure/target/

CORFU_VERSION=$(./mvnw -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)

cp -r ./cmdlets/target/cmdlets-"${CORFU_VERSION}"-shaded.jar ./infrastructure/target/
cp -r ./corfudb-tools/target/corfudb-tools-"${CORFU_VERSION}"-shaded.jar ./infrastructure/target/

# Docker build
cd ./infrastructure

CMD_LINE="docker build "
CMD_LINE+="--network=host "
CMD_LINE+="--build-arg CORFU_JAR=infrastructure-${CORFU_VERSION}-shaded.jar "
CMD_LINE+="--build-arg CMDLETS_JAR=cmdlets-${CORFU_VERSION}-shaded.jar "
CMD_LINE+="--build-arg CORFU_TOOLS_JAR=corfudb-tools-${CORFU_VERSION}-shaded.jar "

if [ -n "$BASE_IMAGE" ]; then
  CMD_LINE+="--build-arg BASE_IMAGE=$BASE_IMAGE "
fi

CMD_LINE+="-t corfudb/corfu-server:cloud "

if [ "$PROFILE" = "docker" ]; then
  CMD_LINE+="-t corfudb/corfu-server:${CORFU_VERSION} -t corfudb-universe/corfu-server:${CORFU_VERSION}"
else
  CMD_LINE+="-t corfudb/corfu-server:latest"
fi

CMD_LINE="$CMD_LINE ."

$CMD_LINE
