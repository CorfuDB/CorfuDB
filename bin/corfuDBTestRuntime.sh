#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

if [ -e "$CORFUDBBIN/../share/corfudb/bin/corfuDBEnv.sh" ]; then
    . "$CORFUDBBINDIR"/../share/corfudb/bin/corfuDBEnv.sh
else
    . "$CORFUDBBINDIR"/corfuDBEnv.sh
fi

if [ "x$1" != "x" ]
then
    echo "Running test: $1"
else
    echo "Usage: $0 <test-name> <args>" >&2
    exit 1
fi

CORFUDB_DAEMON_OUT="/var/log/corfudb.${1}.log"
CORFUDBMAIN="org.corfudb.runtime.$1"

# cjr: 2/3/2015: enclosing the ${*:2} argument in quotes as
# in the original code below causes all the arguments to be flattened
# into a single string, which frustrates the process of unpacking arguments
# with standard tools (gnu getopt won't parse it).
#
#"$JAVA" -cp "$CLASSPATH" $JVMFLAGS "$CORFUDBMAIN" "${*:2}" 2>&1 < /dev/null
#

"$JAVA" -cp "$CLASSPATH" $JVMFLAGS "$CORFUDBMAIN" ${*:2} 2>&1 < /dev/null
