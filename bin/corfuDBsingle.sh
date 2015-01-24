#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

if [ -e "$CORFUDBBIN/../libexec/corfuDBEnv.sh" ]; then
    . "$CORFUDBBINDIR"/../libexec/corfuDBEnv.sh
else
    . "$CORFUDBBINDIR"/corfuDBEnv.sh
fi

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
fi

$CORFUDBBINDIR/corfuDBLaunch.sh sequencer $1
$CORFUDBBINDIR/corfuDBLaunch.sh logunit $1
$CORFUDBBINDIR/corfuDBLaunch.sh configmaster $1

