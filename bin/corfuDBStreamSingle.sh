#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

if [ -e "$CORFUDBBIN/../libexec/corfuDBEnv.sh" ]; then
    . "$CORFUDBBINDIR"/../libexec/corfuDBEnv.sh
else
    . "$CORFUDBBINDIR"/corfuDBEnv.sh
fi

if [[ "$1" != "debug" && "$#" -ne 1 ]]; then
    echo "Usage: $0 {start|debug <start-port-num>|stop|restart|status}"
    exit 1
fi

start_port=$2
if [ "$#" -eq 1 ]
then
    $CORFUDBBINDIR/corfuDBLaunch.sh streaming_sequencer $1
    $CORFUDBBINDIR/corfuDBLaunch.sh logunit $1
    $CORFUDBBINDIR/corfuDBLaunch.sh streaming_configmaster $1
else
    $CORFUDBBINDIR/corfuDBLaunch.sh streaming_sequencer $1 $((start_port))
    $CORFUDBBINDIR/corfuDBLaunch.sh logunit $1 $((start_port+1))
    $CORFUDBBINDIR/corfuDBLaunch.sh streaming_configmaster $1 $((start_port+2))
fi
