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
    CORFUDBCFGDIR="$CORFUDBBINDIR/../etc/corfudb"
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
    CORFUDB_LOG4J_PROP="INFO,CONSOLE"
fi

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

#add the corfudbcfg dir to classpath
CLASSPATH="$CORFUDBCFGDIR:$CLASSPATH"

for i in "$CORFUDBBINDIR"/../src/java/lib/*.jar
do
    CLASSPATH="$i:$CLASSPATH"
done

#make it work in the binary package
#(use array for LIBPATH to account for spaces within wildcard expansion)
if ls "${CORFUDB_PREFIX}"/share/corfudb/corfudb-*.jar > /dev/null 2>&1; then 
  LIBPATH=("${CORFUDB_PREFIX}"/share/corfudb/*.jar)
else
  #release tarball format
  for i in "$CORFUDBDIR"/../corfudb-*.jar
  do
    CLASSPATH="$i:$CLASSPATH"
  done
  LIBPATH=("${CORFUDBBINDIR}"/../lib/*.jar)
fi

for i in "${LIBPATH[@]}"
do
    CLASSPATH="$i:$CLASSPATH"
done

#make it work for developers
#for d in "$CORFUDBBINDIR"/../target/lib/*.jar
#do
#   CLASSPATH="$d:$CLASSPATH"
#done

#make it work for developers
#CLASSPATH="$CORFUDBBINDIR/../target/classes:$CLASSPATH"

CLASSPATH="$CORFUDBBINDIR/../target/corfu-lib-0.1-SNAPSHOT-shaded.jar:$CLASSPATH"
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
