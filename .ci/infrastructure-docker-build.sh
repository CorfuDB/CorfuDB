#!/usr/bin/env bash

set -x
set -e

PROFILE=$1
BASE_IMAGE=$2
if [ -z "$PROFILE" ]; then
  echo "Usage: './infrastructure-docker-build.sh <profile>', available profiles: docker, compatibility"
  exit 1
fi

echo "=========================================="
echo "DEBUG: Starting infrastructure-docker-build.sh"
echo "DEBUG: PROFILE=$PROFILE"
echo "DEBUG: BASE_IMAGE=$BASE_IMAGE"
echo "DEBUG: Current directory: $(pwd)"
echo "DEBUG: Java version:"
java -version
echo "DEBUG: Maven version:"
./mvnw --version
echo "=========================================="

# Clean up project
echo "DEBUG: Cleaning project..."
./mvnw clean

echo "=========================================="
echo "DEBUG: Checking Maven local repository for Maven artifacts BEFORE build"
echo "DEBUG: Listing org.apache.maven artifacts in ~/.m2/repository:"
find ~/.m2/repository/org/apache/maven -name "*.jar" 2>/dev/null | head -20 || echo "No Maven artifacts found yet"
echo "=========================================="

echo "DEBUG: Starting Maven build with compiler plugin..."
echo "DEBUG: This will build cmdlets, corfudb-tools, and infrastructure modules"

# Build cmdlets - Add verbose output and debug flags
echo "DEBUG: Building with Maven..."
./mvnw -pl cmdlets,corfudb-tools,infrastructure --also-make \
  -DskipTests \
  -Dmaven.javadoc.skip=true \
  -T 1C \
  -X \
  -e \
  package 2>&1 | tee /tmp/maven-build-debug.log

echo "=========================================="
echo "DEBUG: Maven build completed"
echo "DEBUG: Checking Maven local repository for Maven artifacts AFTER build"
find ~/.m2/repository/org/apache/maven -name "*.jar" 2>/dev/null | head -30 || echo "No Maven artifacts found"
echo "=========================================="

# Copy corfu binaries into target directory
echo "DEBUG: Copying binaries to infrastructure/target/"
cp -r ./bin ./infrastructure/target/
cp -r ./corfu_scripts ./infrastructure/target/
cp -r ./scripts ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/logback.prod.xml ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/compactor-logback.prod.xml ./infrastructure/target/
cp -r ./infrastructure/src/main/resources/corfu-compactor-config.yml ./infrastructure/target/

CORFU_VERSION=$(./mvnw -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
echo "DEBUG: CORFU_VERSION=$CORFU_VERSION"

cp -r ./cmdlets/target/cmdlets-"${CORFU_VERSION}"-shaded.jar ./infrastructure/target/
cp -r ./corfudb-tools/target/corfudb-tools-"${CORFU_VERSION}"-shaded.jar ./infrastructure/target/

# Docker build
cd ./infrastructure

echo "=========================================="
echo "DEBUG: Building Docker image"
echo "DEBUG: Current directory: $(pwd)"
echo "=========================================="

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

echo "DEBUG: Docker build command: $CMD_LINE"
$CMD_LINE

echo "=========================================="
echo "DEBUG: Build completed successfully!"
echo "=========================================="
