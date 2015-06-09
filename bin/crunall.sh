#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"

. "$CORFUDBBINDIR"/corfuDBEnv.sh

usage="--cmd {start|stop|restart|status} [--config <conf-dir>] [--baseport <port>] [--unitcnt <cnt>]"

baseport=8000
cnt=1
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
	elif [ "--configdir" == "$1" ]
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

echo "running cmd $cmd config-dir $cfgdir baseport $baseport cnt $cnt" >&2
if [ X$cmd = X"" ] 
then
	echo "Usage: $0 $usage" >&2
	exit 1
fi
# ............................
TMPCFGDIR="/var/tmp"

# sequencer:
# ..............
role=sequencer
unit=sequencer
seqcfg="$TMPCFGDIR/corfudb.$role.yml"
if [ -f $seqcfg ]; then
	rm -f $seqcfg
fi
$CORFUDBBINDIR/cgenconfig.sh --role $role --configfile $seqcfg --baseport $baseport --cnt $cnt
$CORFUDBBINDIR/crun.sh --configdir $TMPCFGDIR --unit $unit --cmd $cmd
#  rm -f $seqcfg

# suite of logunits:
# ................
for (( port=$baseport+2; port < $baseport+2+$cnt; port++ ))
do
	role="logunit"
	unit="logunit.$port"
	lgcfg="$TMPCFGDIR/corfudb.$unit.yml" 
	if [ -f $lgcfg ]; then
		rm -f $lgcfg
	fi
	$CORFUDBBINDIR/cgenconfig.sh --role $role --configfile $lgcfg --baseport $baseport --cnt $cnt --port $port
 	$CORFUDBBINDIR/crun.sh --configdir $TMPCFGDIR --unit $unit --cmd $cmd 
	#  rm -f $lgcfg
done

# configmaster:
# ..............
role=master
unit=configmaster
mastercfg="$TMPCFGDIR/corfudb.$unit.yml"
if [ -f $mastercfg ]; then
	rm -f $mastercfg
fi
$CORFUDBBINDIR/cgenconfig.sh --role $role --configfile $mastercfg --baseport $baseport --cnt $cnt
$CORFUDBBINDIR/crun.sh --configdir $TMPCFGDIR --unit $unit --cmd $cmd

