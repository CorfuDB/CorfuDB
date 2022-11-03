#!/usr/bin/env bash

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

CORFUDBBINDIR="${CORFUDBBINDIR:-/usr/bin}"
CORFUDB_PREFIX="${CORFUDBBINDIR}/.."

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

if ls "${DIR}"/../target/*.jar > /dev/null 2>&1; then
  # echo "Running from development source"
  CLASSPATH=("${DIR}"/../infrastructure/target/infrastructure-*-shaded.jar)
else
  CLASSPATH=("${CORFUDB_PREFIX}"/share/corfu/lib/*.jar)
fi

# Windows (cygwin) support
case "$(uname)" in
    CYGWIN*) cygwin=true ;;
    *) cygwin=false ;;
esac

if $cygwin
then
    CLASSPATH=$(cygpath -wp "$CLASSPATH")
fi

if [ "$METRICS_CONFIG_FILE" != "" ]; then
  LOGBACK_CONFIGURATION="-Dlogback.configurationFile=${METRICS_CONFIG_FILE}"
  JAVA="$JAVA $LOGBACK_CONFIGURATION"
fi


#  Jacoco code coverage agent
if [ "$CODE_COVERAGE" = true ]; then
  MVN_PATH="$("${DIR}"/../mvnw help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)"
  JACOCO_VERSION=$("${DIR}"/../mvnw help:evaluate -Dexpression=jacoco.version -q -DforceStdout)
  JACOCO_CMD="-javaagent:${MVN_PATH}/org/jacoco/org.jacoco.agent/${JACOCO_VERSION}/org.jacoco.agent-${JACOCO_VERSION}-runtime.jar=destfile=${DIR}/../infrastructure/target/jacoco-corfu-server.exec,append=true"
  JAVA="$JAVA $JACOCO_CMD"
fi

# default heap for corfudb

CORFUDB_HEAP="${CORFUDB_HEAP:-2000}"
export JVMFLAGS="-XX:+UseG1GC -Xmx${CORFUDB_HEAP}m $SERVER_JVMFLAGS"

$JAVA -cp "$CLASSPATH" $JVMFLAGS org.corfudb.infrastructure.CorfuServer $*
