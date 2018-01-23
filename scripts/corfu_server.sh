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

# setup the corfudb heap size by taking a partition of total memory
if [ ! -z $CORFU_HEAP_SIZE_PERCENT ] && [ $CORFU_HEAP_SIZE_PERCENT -gt 0 ] && [ $CORFU_HEAP_SIZE_PERCENT -lt 100 ]; then
    TOTAL_MEMORY_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
    TOTAL_MEMORY_MB=$(($TOTAL_MEMORY_KB/1024))
    CORFUDB_HEAP=$(($TOTAL_MEMORY_MB*$CORFU_HEAP_SIZE_PERCENT/100))
fi

# default heap for corfudb
CORFUDB_HEAP="${CORFUDB_HEAP:-2000}"
CORFUDB_GC_FLAGS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/var/log/corfu.gc.log"
export JVMFLAGS="-Xmx${CORFUDB_HEAP}m $CORFUDB_GC_FLAGS $SERVER_JVMFLAGS"

# yourkit profiler setup for corfu server
if [ -f /usr/yourkit/bin/libyjpagent.so ] && [ ! -z $CORFU_ENABLE_PROFILER ]; then
    PROFILER_AGENT="-agentpath:/usr/yourkit/bin/libyjpagent.so=port=54322,dir=/var/log,snapshot_name_format=corfu-{sessionname}-{datetime}-{pid}"
else
    PROFILER_AGENT=""
fi

if [[ $* == *--agent* ]]
then
      byteman="-javaagent:"${BYTEMAN_HOME}"/lib/byteman.jar=listener:true"
else
      byteman=""
fi

LOG_PATH=""
# These are the possible options/flags which can be passed to the CorfuServer. The trailing ":" signifies that the flag
# accepts a variable (eg. filename or log directory path, etc)
while getopts "l:ma:nsd:t:c:p:M:eu:f:r:w:bgo:j:k:T:i:H:I:x:z:" opt; do
  case ${opt} in
    l ) LOG_PATH=${OPTARG};;
    ? ) ;;
  esac
done

while true; do
    "$JAVA" -cp "$CLASSPATH" $JVMFLAGS $byteman org.corfudb.infrastructure.CorfuServer $*
    RETURN_CODE=$?

    if [ $RETURN_CODE -ne 100 ] && [ $RETURN_CODE -ne 200 ]; then
        break
    fi

    if [ "$LOG_PATH" != "" ] && [ $RETURN_CODE -eq 100 ]; then
        rm -r $LOG_PATH/*
    fi
done
