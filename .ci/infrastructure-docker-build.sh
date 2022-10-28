#!/usr/bin/env bash

set -e

PROFILE=$1
ENVIRONMENT=$2
if [ -z "$PROFILE" ]; then
  echo "Please provide a profile name: 'docker' or 'compatibility'"
  exit 1
fi

# Clean up project
./mvnw clean

# Build cmdlets
./mvnw -pl cmdlets,corfudb-tools,infrastructure --also-make -DskipTests -Dmaven.javadoc.skip=true -T 1C package

# Copy corfu binaries into target directory
cp -r ./bin ./infrastructure/target/
cp -r ./corfu_scripts ./infrastructure/target/
cp -r ./scripts ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/logback.prod.xml ./infrastructure/target/

touch ./infrastructure/target/libyjpagent.so

if [ "$ENVIRONMENT" = "debug" ]; then
  # Check for the cached version of the YourKit Agent
  if [ ! -f .ci/lib/libyjpagent.so ]; then
    # Download and extract the YourKit Agent and move it to the target directory
    curl --create-dirs -LO --output-dir .ci/lib/ https://www.yourkit.com/download/docker/YourKit-JavaProfiler-2022.3-docker.zip
    (cd .ci/lib/ && unzip YourKit-*.zip && rm YourKit-*.zip && mv */bin/linux-x86-64/libyjpagent.so .)
  fi

  cp .ci/lib/libyjpagent.so ./infrastructure/target/
fi


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
CMD_LINE+="--build-arg YK=libyjpagent.so "

if [ "$PROFILE" = "docker" ]; then
  CMD_LINE+="-t corfudb/corfu-server:${CORFU_VERSION} -t corfudb-universe/corfu-server:${CORFU_VERSION}"
else
  CMD_LINE+="-t corfudb/corfu-server:latest"
fi

CMD_LINE="$CMD_LINE ."

$CMD_LINE
