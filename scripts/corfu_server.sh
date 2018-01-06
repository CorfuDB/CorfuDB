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


# default heap for corfudb
CORFUDB_HEAP="${CORFUDB_HEAP:-2000}"
export JVMFLAGS="-Xmx${CORFUDB_HEAP}m $SERVER_JVMFLAGS"

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
