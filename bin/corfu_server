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

if ls "${DIR}"/../cmdlets/target/*.jar > /dev/null 2>&1; then
  CLASSPATH=("${DIR}"/../cmdlets/target/cmdlets-*-shaded.jar)
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
CORFUDB_HEAP="${CORFUDB_HEAP:-1000}"
export CORFUDB_JVMFLAGS="-Xmx${CORFUDB_HEAP}m $SERVER_JVMFLAGS"

RUN_AS=`basename $0`
"$JAVA" -cp "$CLASSPATH" $JVMFLAGS org.corfudb.shell.ShellMain run-script "$DIR/../corfu_scripts/$RUN_AS.clj" $*
