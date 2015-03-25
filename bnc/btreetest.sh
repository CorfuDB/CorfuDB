#!/bin/bash
(set -o igncr) 2>/dev/null && set -o igncr; # this comment is required
# Above line makes this script ignore Windows line endings, to avoid this error without having to run dos2unix:
#   $'\r': command not found
# As per http://stackoverflow.com/questions/14598753/running-bash-script-in-cygwin-on-windows-7

# btreetest.sh [--iter i] [--verbose] [--type t] [--masternode m] [--help]


# ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 -v -p 9091 -n 50 -k 20 -r 0.25 -x -i bnc/initfs_tiny.ser -w bnc/wkldfs_tiny.ser -h 5 -C CDBLogicalBTree
# ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A playback -t 1 -v -p 9091 -n 50 -k 20 -r 0.25 -x -i bnc/initfs_tiny.ser -w bnc/wkldfs_tiny.ser -h 5 -C CDBLogicalBTree
# ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A crash -z 20 -t 1 -v -p 9091 -n 50 -k 20 -r 0.25 -x -i bnc/initfs_small_A.ser -w bnc/wkldfs_small_A.ser -h 6 -C CDBLogicalBTree

let SUCCEEDED=0
let FAILURES=0
let UNSUPPORTED=0

usage() {
  echo "BTreeFS benchmark usage:"
  echo "btreetest.sh [--iter i] [--verbose] [--type t] [--masternode m] [--help] [--collectstats] [--record]"
  echo "--iter i:        run each case i times"
  echo "--masternode m:  master node (default = http://localhost:8002"
  echo "--verbose:       extra output"
  echo "--type t:        runs only tests for the given list class"
  echo "--help:          prints this message"
}

ITERATIONS=1
specificclass=""
masternode="http://localhost:8002/corfu"
verbose="FALSE"
showoutput=0
readmore=1
help=0
record=0

until [ -z $readmore ]; do
  readmore=
  if [ "x$1" == "x--help" ]; then
    help=1
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
  if [ "x$1" == "x--record" ]; then
    record=TRUE
    echo "record mode"
	shift
	readmore=1
  fi
done

recordWorkload() {
  class=$1
  ops=$2
  case=${3}
  height=${4}
  verbose=${5}
  xdebug=${6}
  ./bin/corfuDBsingle.sh start > /dev/null 2>&1
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 $verbose -p 9091 -n $ops -k 20 -r 0.25 -x -i bnc/initfs_$case.ser -w bnc/wkldfs_$case.ser -h $height -C $class
  ./bin/corfuDBsingle.sh stop > /dev/null 2>&1
}

crashRecoverTest() {
  class=$1
  size=${2}
  crashop=${3}
  recoverop=${4}
  verbose=${5}
  xdebug=${6}
  stats=${7}
  threads=${8}

  ./bin/corfuDBsingle.sh start > /dev/null 2>&1
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -C $class -A crash -z $crashop -t $threads -n 20 -k 20 -r 0.25 $verbose $xdebug -p 9091 -i bnc/initfs_$size.ser -w bnc/wkldfs_$size.ser $stats > bnc/data/crash-$size-$class-t$threads.txt
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -C $class -A recover -z $recoverop -t $threads -n 20 -k 20 -r 0.25 $verbose $xdebug -p 9091 -i bnc/initfs_$size.ser -w bnc/wkldfs_$size.ser $stats -L bnc/data/crash-$size-$class-t$threads.txt > bnc/data/recover-$size-$class-t$threads.txt
  ./bin/corfuDBsingle.sh stop > /dev/null 2>&1

  cat bnc/data/crash-$size-$class-t$threads.txt > tmp.txt
  cat bnc/data/recover-$size-$class-t$threads.txt >> tmp.txt
  echo $size-$class-t$threads >> bnc/data/tput-all.csv
  grep TPUT tmp.txt >> bnc/data/tput-all.csv
  grep TPUT tmp.txt > bnc/data/btree-tput-$size-$class-t$threads.csv
  grep FSREQLAT tmp.txt > bnc/data/btree-reqlat-$size-$class-t$threads.csv
  grep FSCMDLAT tmp.txt >> bnc/data/btree-reqlat-$size-$class-t$threads.csv
}

./bin/corfuDBsingle.sh stop > /dev/null 2>&1
if [ "$record" = "TRUE" ]; then
  echo "recording workloads..."
  #recordWorkload CDBLogicalBTree 30 miniscule 4 -v -x
  #recordWorkload CDBLogicalBTree 50 tiny 5 -v -x
  recordWorkload CDBLogicalBTree 100 small 7 -v -x
  #recordWorkload CDBLogicalBTree 1000 medium 10 -v -x
  #recordWorkload CDBLogicalBTree 10000 large 16 -v -x
  exit
fi

#for size in miniscule tiny small medium large; do
#for class in CDBLogicalBTree CDBPhysicalBTree; do

echo "" > bnc/data/tput-all.csv
for threads in 1; do
for size in miniscule tiny small; do
for class in CDBLogicalBTree CDBPhysicalBTree; do

crashRecoverTest $class $size 20 21 -v -x -S $threads

done
done
done