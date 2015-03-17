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

$CORFUDBBINDIR/corfuDBLaunch.sh multilog_sequencer_1 $1
$CORFUDBBINDIR/corfuDBLaunch.sh multilog_logunit_1 $1
$CORFUDBBINDIR/corfuDBLaunch.sh multilog_configmaster_1 $1
$CORFUDBBINDIR/corfuDBLaunch.sh multilog_sequencer_2 $1
$CORFUDBBINDIR/corfuDBLaunch.sh multilog_logunit_2 $1
$CORFUDBBINDIR/corfuDBLaunch.sh multilog_configmaster_2 $1

