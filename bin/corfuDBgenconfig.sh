#!/usr/bin/env bash
# script for generating config files
# command line params are:
usage="--type {master|sequencer|logunit} --baseport <port #> -cnt <# logunits> --config <filename> [--port <port>]"

baseport=0
cnt=0
type="illegal"
cfg=""

# parse arguments
while  [ $# -ge 2 ] 
do
	if [ "--baseport" = "$1" ]
	then
		shift
		baseport=$1
		shift
	elif [ "--cnt" == "$1" ]
	then
		shift
		cnt=$1
		shift
	elif [ "--type" == "$1" ]
	then
		shift
		type=$1
		shift
	elif [ "--config" == "$1" ]
	then
		shift
		cfg=$1
		shift
	elif [ "--port" == "$1" ]
	then
		shift
		port=$1
		shift
	else
		echo "unrecognized command-line switch: $1"
		echo "Usage: $0 $usage"
		exit 1
	fi
done

echo "building config type $type config-file $cfg baseport $baseport cnt $cnt"
if [ $baseport -eq 0 ]  || [ $cnt -eq 0 ] || [ $type = "illegal" ] || [ X$cfg = X"" ] || ( [ $type == "logunit" ] && [ X$port = X"" ] )
then
	echo "Usage: $0 $usage"
	exit 1
fi

####################################

case $type in

# ........................................
master)
echo creating master config-file

cat > $cfg << _EOF
role: org.corfudb.infrastructure.ConfigMasterServer
port: $baseport
pagesize: 4096
epoch: 0
sequencers:
  - "cdbss://localhost:$(( $baseport+1))"
configmasters:
  - "cdbcm://localhost:$baseport"
layout:
  segments:
    - start: 0
      sealed: -1
      replicas: $cnt
      groups:
        - nodes:
_EOF

for (( port=$baseport+2; port < $baseport+2+$cnt; port++ ))
do
cat >> $cfg << _EOF
          - "cdbslu://localhost:$port"
_EOF
done

;;

# ........................................
logunit)
cat > $cfg << _EOF
role: org.corfudb.infrastructure.SimpleLogUnitServer
port: $port
capacity: 10000
ramdisk: true
pagesize: 4096
trim: 0
master: http://localhost:$baseport/corfu
_EOF

;;

# ........................................
sequencer)
cat > $cfg << _EOF
role: org.corfudb.infrastructure.SimpleSequencerServer
port: $(( baseport+1 ))
_EOF

;;


esac
