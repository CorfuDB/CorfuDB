#!/usr/bin/env bash

# This file is based on Apache ZooKeeper's zkEnv.sh

# We use CORFUDBCFGDIR if defined,
# otherwise we use /etc/corfudb
# or the conf directory that is
# a sibling of this script's directory.
# Or you can specify the CORFUDBCFGDIR using the
# '--config' option in the command line.

CORFUDBBINDIR="${CORFUDBBINDIR:-/usr/bin}"
CORFUDB_PREFIX="${CORFUDBBINDIR}/.."

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      CORFUDBCFGDIR=$confdir
    fi
fi

if [ "x$CORFUDBCFGDIR" = "x" ]
then
  if [ -e "${CORFUDB_PREFIX}/conf" ]; then
    CORFUDBCFGDIR="$CORFUDBBINDIR/../conf"
  else
    CORFUDBCFGDIR="/etc/corfudb"
  fi
fi

if [ "x$CORFUDBCFG" = "x" ]
then
    CORFUDBCFG="corfudb.yml"
fi

CORFUDBCFG="$CORFUDBCFGDIR/$CORFUDBCFG"

if [ -f "$CORFUDBCFGDIR/java.env" ]
then
    . "$CORFUDBCFGDIR/java.env"
fi

if [ "x${CORFUDB_LOG_DIR}" = "x" ]
then
    CORFUDB_LOG_DIR="."
fi

if [ "x${CORFUDB_LOG4J_PROP}" = "x" ]
then
    CORFUDB_LOG4J_PROP="DEBUG"
fi

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

#add the corfudbcfg dir to classpath
CLASSPATH="$CORFUDBCFGDIR:$CLASSPATH"

if ls "$CORFUDBBINDIR"/../target/*.jar > /dev/null 2>&1; then
  echo "Running from development source"
  CLASSPATH=("$CORFUDBBINDIR"/../target/corfudb-*-shaded.jar)
elif ls "${CORFUDB_PREFIX}"/share/corfudb/lib/corfudb-*.jar > /dev/null 2>&1; then
  CLASSPATH=("${CORFUDB_PREFIX}"/share/corfudb/lib/*.jar)
fi


case "`uname`" in
    CYGWIN*) cygwin=true ;;
    *) cygwin=false ;;
esac

if $cygwin
then
    CLASSPATH=`cygpath -wp "$CLASSPATH"`
fi

#echo "CLASSPATH=$CLASSPATH"

# default heap for corfudb 
CORFUDB_HEAP="${CORFUDB_HEAP:-1000}"
export CORFUDB_JVMFLAGS="-Xmx${CORFUDB_HEAP}m $SERVER_JVMFLAGS"
