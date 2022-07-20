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
case "`uname`" in
    CYGWIN*) cygwin=true ;;
    *) cygwin=false ;;
esac

if $cygwin
then
    CLASSPATH=`cygpath -wp "$CLASSPATH"`
fi

if [ "$METRICS_CONFIG_FILE" != "" ]; then
  LOGBACK_CONFIGURATION="-Dlogback.configurationFile=${METRICS_CONFIG_FILE}"
  JAVA="$JAVA $LOGBACK_CONFIGURATION"
fi

# default heap for corfudb
CORFUDB_HEAP="${CORFUDB_HEAP:-2000}"
export JVMFLAGS="-Xmx${CORFUDB_HEAP}m $SERVER_JVMFLAGS"
export MVN_PATH="$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)"
#export JACOCO="-javaagent:/Users/huaca/.m2/repository/org/jacoco/org.jacoco.agent/0.8.8/org.jacoco.agent-0.8.8-runtime.jar=output=tcpclient,address=127.0.0.1,port=6300,append=true,sessionid=lr,includes=*"
export JACOCO="-javaagent:${MVN_PATH}/org/jacoco/org.jacoco.agent/0.8.8/org.jacoco.agent-0.8.8-runtime.jar=destfile=${DIR}/../infrastructure/target/jacoco-lr-server.exec"

$JAVA $JACOCO -cp "$CLASSPATH" $JVMFLAGS org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer $*
