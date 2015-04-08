#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

if [ -e "$CORFUDBBIN/../share/corfudb/bin/corfuDBEnv.sh" ]; then
    . "$CORFUDBBINDIR"/../share/corfudb/bin/corfuDBEnv.sh
else
    . "$CORFUDBBINDIR"/corfuDBEnv.sh
fi

if [ "x$1" == "x" ]; then
    echo "Usage: $0 <test-name> <args>" >&2
    exit 1
fi

CORFUDB_DAEMON_OUT="/var/log/corfudb.${1}.log"
CORFUDBMAIN="org.corfudb.tests.benchtests.MicroBenchmark"


#echo " -cp "$CLASSPATH" $JVMFLAGS "$CORFUDBMAIN" -A ${1} ${*:2}"
"$JAVA" -cp "$CLASSPATH" $JVMFLAGS "$CORFUDBMAIN" -A ${1} ${*:2} 2>&1

