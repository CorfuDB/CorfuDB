#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

. "$CORFUDBBINDIR"/corfuDBEnv.sh

usage="--cmd {start|stop|restart|status} [--config <conf-dir>] [--baseport <port>] [--unitcnt <cnt>]"

baseport=8000
unitcnt=1
cmd=""
cfgdir=$CORFUDBCFGDIR

# parse arguments
while  [ $# -ge 2 ] 
do
	if [ "--baseport" = "$1" ]
	then
		shift
		baseport=$1
		shift
	elif [ "--unitcnt" == "$1" ]
	then
		shift
		cnt=$1
		shift
	elif [ "--config" == "$1" ]
	then
		shift
		cfgdir=$1
		shift
	elif [ "--cmd" == "$1" ]
	then
		shift
		cmd=$1
		shift
	else
		echo "unrecognized command-line switch: $1" >&2
		echo "Usage: $0 $usage" >&2
		exit 1
	fi
done

echo "running cmd $cmd config-dir $cfgdir baseport $baseport cnt $unitcnt" >&2
if [ X$cmd = X"" ] 
then
	echo "Usage: $0 $usage" >&2
	exit 1
fi
# ............................
TMPCFGDIR="/var/tmp"

# generate sequencer.yml:
# ----------------------------
role=sequencer
seqcfg="$TMPCFGDIR/corfudb.$role.yml"
if [ -f $seqcfg ]; then
	rm -f $seqcfg
fi
$CORFUDBBINDIR/fugenconfig.sh --role $role --config $seqcfg --baseport $baseport --cnt $cnt
$CORFUDBBINDIR/furun.sh --config $TMPCFGDIR --role $role --cmd $cmd
#  rm -f $seqcfg

role=configmaster
mastercfg="$TMPCFGDIR/corfudb.$role.yml"
if [ -f $mastercfg ]; then
	rm -f $mastercfg
fi
$CORFUDBBINDIR/fugenconfig.sh --role $role --config $mastercfg --baseport $baseport --cnt $cnt
$CORFUDBBINDIR/furun.sh --config $TMPCFGDIR --role $role --cmd $cmd

# generate series of ft_logunit#.yml:
# ----------------------------
for (( port=$baseport+2; port < $baseport+2+$cnt; port++ ))
do
	lgcfg="$TMPCFGDIR/corfudb.logunit$port.yml" 
	if [ -f $lgcfg ]; then
		rm -f $lgcfg
	fi
	$CORFUDBBINDIR/fugenconfig.sh --role logunit --config $lgcfg --baseport $baseport --cnt $cnt --port $port
 	$CORFUDBBINDIR/furun.sh --config $TMPCFGDIR --role logunit --port "logunit$port" --cmd $cmd 
	#  rm -f $lgcfg
done
