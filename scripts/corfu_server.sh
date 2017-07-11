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
  CLASSPATH=("${DIR}"/../debian/target/nsx-corfu-*-shaded.jar)
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


# default heap for corfudb
CORFUDB_HEAP="${CORFUDB_HEAP:-2000}"
CORFUDB_GC_FLAGS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/var/log/corfu.gc.log"
export JVMFLAGS="-Xmx${CORFUDB_HEAP}m $CORFUDB_GC_FLAGS $SERVER_JVMFLAGS"

# yourkit profiler setup for corfu server
if [ -f /usr/yourkit/bin/libyjpagent.so ] && [ -n $CORFU_ENABLE_PROFILER ]; then
    PROFILER_AGENT="-agentpath:/usr/yourkit/bin/libyjpagent.so=port=54322,dir=/var/log,snapshot_name_format=corfu-{sessionname}-{datetime}-{pid}"
else
    PROFILER_AGENT=""
fi

while true; do
    "$JAVA" ${PROFILER_AGENT} -cp "$CLASSPATH" $JVMFLAGS org.corfudb.infrastructure.CorfuServer $*
    JAVA_RET_CODE=$?
    # watchdog: only exit on INTERNAL_ERROR (1), SIGHUP (129), SIGINT (130), SIGKILL (137) and SIGTERM (143)
    EXIT_CONDITION="1 129 130 137 143"
    if [[ " $EXIT_CONDITION " =~ " $JAVA_RET_CODE " ]]; then
        break
    fi
done
