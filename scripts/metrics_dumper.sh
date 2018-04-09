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



usage() { echo "Usage: $0 [-d (uses default flags)] [-j <corfu.jmx.service.url>] [-m <dump.domains>] [-f <dump.file>]
default setting: (-Dcorfu.jmx.service.url=service:jmx:rmi:///jndi/rmi://localhost:6666/jmxrmi -Ddump.domains=corfu.metrics -Ddump.file=/tmp/metrics-dump)
corfu.jmx.service.url: the jmx service url. It should be in the form of (service:jmx:rmi:///jndi/rmi://ip-address:port/jmxrmi)
dump.domains: domains to be dumped. It should be in the form of comma separated string. If empty, all domains will be dumped.
dump.file: path to file for the dump." 1>&2; exit 1; }

default=""
jmxserver=""
metric=""
dump=""

while getopts ":j:m:f:d" opt; do
    case $opt in
        d) default="-Dcorfu.jmx.service.url=service:jmx:rmi:///jndi/rmi://localhost:6666/jmxrmi -Ddump.domains=corfu.metrics -Ddump.file=/tmp/metrics-dump"
        echo "Using default flags: $default"
        ;;
        j) jmxserver="-Dcorfu.jmx.service.url=$OPTARG"
        ;;
        m) metric="-Ddump.domains=$OPTARG"
        ;;
        f) dump="-Ddump.file=$OPTARG"
        ;;
        *) usage
        ;;
    esac
done

# default heap for dumper
DUMPER_HEAP="${DUMPER_HEAP:-256}"
export JVMFLAGS="-Xmx${DUMPER_HEAP}m $default $jmxserver $metric $dump"

echo $JVMFLAGS

RUN_AS=`basename $0`
"$JAVA" -cp "$CLASSPATH" $JVMFLAGS org.corfudb.metrics.MBeanDumperApp $*