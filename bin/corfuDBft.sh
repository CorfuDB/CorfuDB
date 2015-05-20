#!/usr/bin/env bash

CORFUDBBIN="${BASH_SOURCE-$0}"
CORFUDBBIN="$(dirname "${CORFUDBBIN}")"
CORFUDBBINDIR="$(cd "${CORFUDBBIN}"; pwd)"


if [ -e "$CORFUDBBIN/../libexec/corfuDBEnv.sh" ]; then
    . "$CORFUDBBINDIR"/../libexec/corfuDBEnv.sh
else
    . "$CORFUDBBINDIR"/corfuDBEnv.sh
fi

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <logunit-cnt> {start|stop|restart|status}"
    exit 1
fi

baseport=8000    # future: make this optional parameter --port
logunitcnt=$1
cmd=$2

# ##############################
# generate temporary configuration files
# ##############################
TMPCFGDIR="/var/tmp"

# generater configmaster.yml:
# ----------------------------
mastercfg="$TMPCFGDIR/configmaster.yml"

if [ -z $mastercfg ]; then
	rm -f $mastercfg
fi
cat > $mastercfg <<- _EOF
	role: org.corfudb.infrastructure.ConfigMasterServer
	port: $(( $baseport + 2))
	pagesize: 4096
	epoch: 0
	sequencers:
	    - "cdbss://localhost:$baseport"
	configmasters:
	    - "cdbcm://localhost:$(($baseport + 2))"
	layout:
	    segments:
	       - start: 0
	         sealed: -1
	         replicas: $logunitcnt
	         groups:
	            - nodes:
_EOF

for (( port=$baseport+3; port < $baseport+3+$logunitcnt; port++ ))
do
	cat >> $mastercfg <<- _EOF
	               - "cdbslu://localhost:$port"
_EOF
done

# generater sequencer.yml:
# ----------------------------
seqcfg="$TMPCFGDIR/sequencer.yml"
if [ -z $seqcfg ]; then
	rm -f $seqcfg
fi
cat > $seqcfg <<- _EOF
	role: org.corfudb.infrastructure.SimpleSequencerServer
	port: $baseport
_EOF


# generater series of ft_logunit#.yml:
# ----------------------------
for (( port=$baseport+3; port < $baseport+3+$logunitcnt; port++ ))
do
	lgcfg="$TMPCFGDIR/ft_logunit$port.yml" 
	if [ -z $lgcfg ]; then
		rm -f $lgcfg
	fi
	cat > $lgcfg <<- _EOF
		role: org.corfudb.infrastructure.SimpleLogUnitServer
		port: $port
		capacity: 10000
		ramdisk: true
		pagesize: 4096
		trim: 0
		master: http://localhost:$(($baseport + 2))/corfu
_EOF
done

# now, 1.. 2.. 3.. launch!
# ###########################

$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR sequencer $cmd

for (( port=$baseport+3; port < $baseport+3+$logunitcnt; port++ ))
do
 	$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR "ft_logunit$port" $cmd 
done

$CORFUDBBINDIR/corfuDBLaunch.sh --config $TMPCFGDIR configmaster $cmd

