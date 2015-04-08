#!/bin/bash
(set -o igncr) 2>/dev/null && set -o igncr; # this comment is required
# Above line makes this script ignore Windows line endings, to avoid this error without having to run dos2unix:
#   $'\r': command not found
# As per http://stackoverflow.com/questions/14598753/running-bash-script-in-cygwin-on-windows-7

# btreetest.sh [--iter i] [--verbose] [--xdebug] [--masternode m] [--port p] [--collectstats]
#    [--streamimpl s] [--help] [--debugscript] [--notx]

let SUCCEEDED=0
let FAILURES=0
let UNSUPPORTED=0
ITERATIONS=1
specificclass="___"
specificstreamimpl="___"
masternode="http://localhost:8002/corfu"
port=9091
verbose="FALSE"
xdebug="FALSE"
collectstats="TRUE"
debugscript="FALSE"
usetx="TRUE"
showoutput=0
readmore=1
help=0
Mparms=""

inform() {
  str=$1
  if [ "$debugscript" == "TRUE" ]; then
    echo $str
  fi
}

usage() {
  echo "ubnc benchmark usage:"
  echo "ubnc.sh [--iter i] [--verbose] [--xdebug] [--masternode m] [--collectstats b] "
  echo "             [--streamimpl s] [--workload c] [--debugscript] [--help]"
  echo "             [--notx] "
  echo "--iter i:           run each case i times"
  echo "--verbose:          extra output"
  echo "--xdebug:           extreme debug output"
  echo "--masternode m:     master node (default = http://localhost:8002)"
  echo "--port p:           client port (default = 9091)"
  echo "--collectstats b:   collect fine-grain request latency stats (1->collect, 0->don't, default=1)"
  echo "--streamimpl s:     use *only* the specified stream implementation [DUMMY|HOP]"
  echo "--debugscript:      emit script commands to console"
  echo "--notx:             disable txns at FS API layer"
  echo "--help:             prints this message"
}

until [ -z $readmore ]; do
  readmore=
  if [ "x$1" == "x--help" ]; then
    help=1
	shift
	readmore=1
  fi
  if [ "x$1" == "x--notx" ]; then
    usetx="FALSE"
    inform "not using txns in fs API"
	shift
	readmore=1
  fi
  if [ "x$1" == "x--masternode" ]; then
    masternode=$2
    inform "masternode = $masternode"
	shift
	shift
	readmore=1
  fi
  if [ "x$1" == "x--iter" ]; then
    ITERATIONS=$2
    inform "task iterations: $ITERATIONS"
	shift
	shift
	readmore=1
  fi
  if [ "x$1" == "x--verbose" ]; then
    verbose="TRUE"
    verboseflags=" -v "
    inform "verbose mode"
	shift
	readmore=1
  fi
  if [ "x$1" == "x--xdebug" ]; then
    xdebug="TRUE"
    xdebugflags=" -x "
    inform "extreme debug mode"
	shift
	readmore=1
  fi
  if [ "x$1" == "x--debugscript" ]; then
    debugscript="TRUE"
    inform "script debugging mode"
	shift
	readmore=1
  fi
  if [ "x$1" == "x--streamimpl" ]; then
    specificstreamimpl=$2
    inform "specific stream impl = $specificstreamimpl"
	shift
	shift
	readmore=1
  fi
  if [ "x$1" == "x--port" ]; then
    port=$2
    inform "port = $port"
	shift
	shift
	readmore=1
  fi
  if [ "x$1" == "x--collectstats" ]; then
    collectstatsparm=$2
    collectstats=" "
    if [ "x$collectstatsparm" == "x1" ]; then
      collectstats=" -S "
    fi
    inform "collectstats = $collectstats"
	shift
	shift
	readmore=1
  fi

done

function getrtflags() {
  local streamlabel=$1
  local __rtflags=$2
  local flags=""
  local verboseflags=""
  local xdebugflags=""
  local statsflags=""
  local streamflags=""
  local txflags=""
  if [ "$usetx" == "FALSE" ]; then
	  txflags="-N"
  fi
  if [ "$streamlabel" == "HOP" ]; then
	  streamflags="-s 1"
  fi
  if [ "$verbose" == "TRUE" ]; then
	  verboseflags="-v"
  fi
  if [ "$xdebug" == "TRUE" ]; then
	  xdebugflags="-x"
  fi
  if [ "$collectstats" == "TRUE" ]; then
	  statsflags="-S"
  fi
  flags="-m $masternode -p $port $txflags $streamflags $verboseflags $xdebugflags $statsflags $Mparms"
  eval $__rtflags="'$flags'"
}

function getscript() {
  local streamlabel=$1
  local __script=$2
  local sscript=""
  if [ "$streamlabel" == "HOP" ]; then
	  sscript="corfuDBMultiple"
  else
	  sscript="corfuDBsingle"
  fi
  eval $__script="'$sscript'"
}

corfucmd() {
  local ccmd=$1
  local astreamlabel=$2
  getscript $astreamlabel corfuscript
  inform "./bin/$corfuscript.sh $ccmd"
  ./bin/$corfuscript.sh $ccmd > /dev/null 2>$1
}

startcorfu() {
  corfucmd start $1
}

stopcorfu() {
  corfucmd stop $1
}

standalonetest() {
  local streamimpl=$1
  local args="${@:2}"
  getrtflags $streamimpl rtflags
  startcorfu $streamimpl
  ./bin/corfuDBTestRuntime.sh CorfuDBTester $rtflags $args
  stopcorfu $streamimpl
}

runtest() {
  inform "runtest"
  local streamimpl=$1
  local args="${@:2}"
  getrtflags $streamimpl rtflags
  local rcmd="./bin/corfuDBTestRuntime.sh CorfuDBTester $rtflags $args"
  inform "$rcmd"
  ./bin/corfuDBTestRuntime.sh CorfuDBTester $rtflags $args
}

function skipitem() {
  local item=$1
  local specificitem=$2
  local __skip=$3
  local shouldskip="FALSE"
  if [ "$specificitem" != "___" ]; then
	if [ "$specificitem" != "$item" ]; then
	  shouldskip="TRUE"
    fi
  fi
  eval $__skip="'$shouldskip'"
}



runMicrobenchmark() {

  local bnc=$1
  local run=$2
  local streamimpl=${3}
  local threads=${4}
  local nobj=${5}
  local nops=${6}
  local osize=${7}
  local csize=${8}
  local rwpct=${9}

  ddir=bnc/ubnc-data
  caselbl=r$run-s$streamimpl-t$threads-oc$nobj=ops$nops-os$osize-cs$csize-rw$rwpct
  dlog="$ddir/$caselbl.txt"
  dcsv="$ddir/tput-all.csv"
  runstart="`date +%Y-%m-%d`"

  inform "running $caselbl:"
  startcorfu $streamimpl
  inform "corfu started, running ubnc..."
  if [ "$verbose" == "TRUE" ]; then
    ./bin/runbnc.sh $bnc -L false -o $nobj -n $nops -c $csize -s $osize -t $threads -S $streamimpl -r $rwpct | tee $dlog
  else
    ./bin/runbnc.sh $bnc -L false -o $nobj -n $nops -c $csize -s $osize -t $threads -S $streamimpl -r $rwpct > $dlog
  fi
  inform "complete...stopping corfu"
  stopcorfu $streamimpl
  inform "...done"

  grep $bnc $dlog | grep -v "Benchmark:" >> $dcsv
}

rm bnc/ubnc-data/*
DATETIME="`date +%Y-%m-%d`"
echo -e "\nCorfuDB ubnc test $DATETIME"
echo -e "-----------------------------------"
#echo -e "\nCorfuDB ubnc test $DATETIME" >> bnc/ubnc-data/ubnc.csv
#echo -e "-----------------------------------" >> bnc/ubnc-data/ubnc.csv

for bnc in CommandThroughput; do
for run in `seq 1 $ITERATIONS`; do
for streamimpl in DUMMY; do # HOP; do
for threads in 1 2 4 8; do
for nobj in 10 100 1000; do
for nops in 2048; do
for cosize in 0 128 2048; do
for rwpct in 0.05 0.95; do

runMicrobenchmark $bnc $run $streamimpl $threads $nobj $nops $cosize $cosize $rwpct


done
done
done
done
done
done
done
done

