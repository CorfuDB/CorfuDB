#!/bin/bash
(set -o igncr) 2>/dev/null && set -o igncr; # this comment is required
# Above line makes this script ignore Windows line endings, to avoid this error without having to run dos2unix:
#   $'\r': command not found
# As per http://stackoverflow.com/questions/14598753/running-bash-script-in-cygwin-on-windows-7

# listtest.sh [--iter i] [--verbose] [--type t] [--masternode m] [--help]
let SUCCEEDED=0
let FAILURES=0
let UNSUPPORTED=0

usage() {
  echo "list benchmark usage:"
  echo "listtest.sh [--iter i] [--verbose] [--type t] [--masternode m] [--help]"
  echo "--iter i:        run each case i times"
  echo "--masternode m:  master node (default = http://localhost:8002"
  echo "--verbose:       extra output"
  echo "--type t:        runs only tests for the given list class"
  echo "--help:          prints this message"
}

BINDIR=`cygpath -wa "$CORFU_ROOT/bin"`
# echo BINDDIR=${BINDIR}
ROOTDIR=`cygpath -w "$CORFU_ROOT"`
ITERATIONS=1
specificclass=""
masternode="http://localhost:8002/corfu"
verbose="FALSE"
showoutput=0
readmore=1
help=0

until [ -z $readmore ]; do
  readmore=
  if [ "x$1" == "x--rootdir" ]; then
    ROOTDIR=`cygpath -u "${2}"`
    echo "changing Corfu source tree root to $ROOTDIR"
	shift
	shift
	readmore=1
  fi      
  if [ "x$1" == "x--help" ]; then
    help=1
	shift
	readmore=1
  fi      
  if [ "x$1" == "x--showoutput" ]; then
    showoutput=1
	shift
	readmore=1
  fi      
  if [ "x$1" == "x--masternode" ]; then
    masternode=$2
    echo "masternode = $masternode"
	shift
	shift
	readmore=1
  fi        
  if [ "x$1" == "x--type" ]; then
    specificclass=$2
    echo "run only $specificclass test cases"
	shift
	shift
	readmore=1
  fi
  if [ "x$1" == "x--iter" ]; then
    ITERATIONS=$2
    echo "task iterations: $ITERATIONS"
	shift
	shift
	readmore=1
  fi    
  if [ "x$1" == "x--verbose" ]; then
    verbose=TRUE
    echo "verbose mode"
	shift
	readmore=1
  fi    
done

if [ "$help" == "1" ]; then 
  usage
  exit 0
fi

testCase() {
  outdir=$1
  iters=${2}
  type=${3}
  thrds=${4}
  masternode=${5}
  keys=${6}
  rwpct=${7}
  ops=${8}

  echo "testCase: iters=$iters, type=$type, thrds=$thrds, keys=$keys, rwpct=$rwpct, ops=$ops"

  if [ "$specificclass" != "" ]; then
	if [ "$specificclass " != "$type" ]; then
	  return
	fi  
  fi
  
  for iter in `seq 1 $iters`; do
    outfile=$outdir/c$type-t$thrds-k$keys-rw$rwpct-o$ops-run$iter.txt
    startcmd="$BINDIR/corfuDBsingle.sh start"
    cmd="$BINDIR/corfuDBTestRuntime.sh CorfuDBTester -m $masternode -a $type -t $thrds -n $ops -k $keys -r $rwpct"
    stopcmd="$BINDIR/corfuDBsingle.sh stop"
    if [ "$verbose" = "TRUE" ]; then
      echo "testCase $outdir, run$iter, $type, thrds=$thrds"
      echo "cmd: $cmd"
      echo "outfile: $outfile"
    fi
    echo "cmd: $cmd" > $outfile 
	echo "" >> $outfile

	$startcmd >> $outfile 2>&1
    $cmd >> $outfile 2>&1
	$stopcmd >> $outfile 2>&1

	resstr="----"
    if ! egrep "PASSED" $outfile > /dev/null; then
      resstr="***FAILED***..."
      FAILURES=$[ $FAILURES+1 ]
	else
	  resstr="OK..."
	fi

    egrep "tput" $outfile

    if [ "$verbose" = "TRUE" ]; then
	  echo "current failure count: $FAILURES"
	  echo "current success count: $SUCCEEDED"
	  echo "current unsupported test case count: $UNSUPPORTED"
    fi	
    if [ "$showoutput" == "1" ]; then
       cat $outfile
    fi
  done
  sleep 2
}


DATETIME="`date +%Y-%m-%d`"  
echo -e "\nCorfuDB list test $DATETIME"
echo -e "-----------------------------------"

outdir=out-$DATETIME
if [ ! -e $outdir ]; then
  mkdir $outdir
else
  rm -f $outdir/*
fi

for Xtype in 6 7 8; do
for Xconcurrency in 1 2 4; do
for Xrwpct in 0.25 0.75 1.0; do
#for Xkeys in 20 80; do
for Xkeys in 40; do
#for Xops in 20 80 160; do
for Xops in 40; do

testCase $outdir $ITERATIONS $Xtype $Xconcurrency $masternode $Xkeys $Xrwpct $Xops

done
done
done
done
done

echo successes: $SUCCEEDED
echo failures: $FAILURES
echo unsupported cases: $UNSUPPORTED

if [ "$FAILURES" != "0" ]; then
  exit 1
fi

