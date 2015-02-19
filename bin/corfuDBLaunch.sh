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
    echo $1
    CORFUDBCFG="$CORFUDBCFGDIR/$1.yml"
else
    echo "Usage: $0 [--config <conf-dir>] <node-name> {start|stop|restart|status}" >&2
fi

if $cygwin
then
    CORFUDBCFG=`cygpath -wp "$CORFUDBCFGDIR/$1.yml"`
    KILL=/bin/kill
else
    KILL=kill
fi

CORFUDB_DAEMON_OUT="/var/log/corfudb.${1}.log"
CORFUDBMAIN="org.corfudb.infrastructure.ConfigParser"

echo "Using config: $CORFUDBCFG" >&2
echo "Logging to: $CORFUDB_DAEMON_OUT" >&2

if [ -z "$CORFUDBPIDFILE" ]; then
    CORFUDBPIDFILE="/var/run/corfudb.${1}.pid"
else
    mkdir -p "$(dirname $CORFUDBPIDFILE)"
fi

case $2 in
start)
    echo -n "Starting CorfuDB role ${1}..."
    if [ -f "$CORFUDBPIDFILE" ]; then
        if kill -0 `cat $CORFUDBPIDFILE` > /dev/null 2>&1; then
            echo $command already running as process `cat "$CORFUDBPIDFILE"`.
            exit 0
        fi
    fi
    nohup "$JAVA" "-Dorg.slf4j.simpleLogger.defaultLogLevel=${CORFUDB_LOG4J_PROP}" \
    -cp "$CLASSPATH" $JVMFLAGS "$CORFUDBMAIN" "$CORFUDBCFG" > "$CORFUDB_DAEMON_OUT" 2>&1 < /dev/null &
    if [ $? -eq 0 ]
    then
        if /bin/echo -n $! > "$CORFUDBPIDFILE"
        then
            sleep 2
            # The server may have failed to start. Let's make sure it did
            if kill -0 $! > /dev/null 2>&1;
            then
                echo Started
            else
                echo Failed to start, log was:
                cat "$CORFUDB_DAEMON_OUT"
            fi
        else
            echo Failed to write PID
            exit 1
        fi
    else
        echo Server failed to start
        exit 1
    fi
    ;;
stop)
    echo -n "Stopping CorfuDB role ${1}..."
    if [ ! -f "$CORFUDBPIDFILE" ];
    then
        echo "Could not find a PID file to stop..."
        exit 0 #should this be exit 1, maybe?
    else
        $KILL $(cat "$CORFUDBPIDFILE")
        rm "$CORFUDBPIDFILE"
        echo Stopped
        exit 0
    fi
    ;;
restart)
    "$0" "$1" stop
    sleep 3
    "$0" "$1" start
    ;;
status)
    if [ ! -f "$CORFUDBPIDFILE" ];
    then
        echo "Could not find a PID file..."
        exit 0 #should this be exit 1, maybe?
    else
        echo -n "CorfuDB role ${1} running as PID "
        echo $(cat "$CORFUDBPIDFILE")
        echo "Last 100 lines of log:"
        cat $CORFUDB_DAEMON_OUT | tail -100
        exit 0
    fi
    ;;
*)
    echo "Usage: $0 [--config <conf-dir>] <node-name> {start|stop|restart|status}" >&2
    ;;
esac
