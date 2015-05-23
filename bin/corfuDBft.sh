#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"


. "$CORFUDBBINDIR"/corfuDBEnv.sh

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 [--baseport <port#>] <logunit-cnt> {start|stop|restart|status}"
    exit 1
fi

#check to see if base port is given as an optional argument
if [ $# -gt 2 ]
then
	if [ "--baseport" = "$1" ]
	then
		shift
		baseport=$1
		shift
	else
		echo "Usage: $0 [--baseport <port#>] <logunit-cnt> {start|stop|restart|status}"
		exit 1
	fi
else
	baseport=8000    
fi  

logunitcnt=$1
cmd=$2

TMPCFGDIR="/var/tmp"

# generate sequencer.yml:
# ----------------------------
seqcfg="$TMPCFGDIR/corfudb.sequencer.yml"
if [ -f $seqcfg ]; then
	rm -f $seqcfg
fi
$CORFUDBBINDIR/corfuDBgenconfig.sh --type sequencer --config $seqcfg --baseport $baseport --cnt $logunitcnt
$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR "sequencer" $cmd
#  rm -f $seqcfg

mastercfg="$TMPCFGDIR/corfudb.configmaster.yml"
if [ -f $mastercfg ]; then
	rm -f $mastercfg
fi
$CORFUDBBINDIR/corfuDBgenconfig.sh --type master --config $mastercfg --baseport $baseport --cnt $logunitcnt
$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR "configmaster" $cmd

# generate series of ft_logunit#.yml:
# ----------------------------
for (( port=$baseport+2; port < $baseport+2+$logunitcnt; port++ ))
do
	lgcfg="$TMPCFGDIR/corfudb.logunit$port.yml" 
	if [ -f $lgcfg ]; then
		rm -f $lgcfg
	fi
	$CORFUDBBINDIR/corfuDBgenconfig.sh --type logunit --config $lgcfg --baseport $baseport --cnt $logunitcnt --port $port
 	$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR "logunit$port" $cmd 
	#  rm -f $lgcfg
done
