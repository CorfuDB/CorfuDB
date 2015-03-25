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

./bin/corfuDBsingle.sh stop

if [ "$record" = "TRUE" ]; then
  echo "recording workloads..."
  ./bin/corfuDBsingle.sh start
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 -v -p 9091 -n 50 -k 20 -r 0.25 -x -i bnc/initfs_tiny.ser -w bnc/wkldfs_tiny.ser -h 5 -C CDBLogicalBTree
  ./bin/corfuDBsingle.sh stop

  ./bin/corfuDBsingle.sh start
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 -v -p 9091 -n 100 -k 20 -r 0.25 -x -i bnc/initfs_small.ser -w bnc/wkldfs_small.ser -h 7 -C CDBLogicalBTree
  ./bin/corfuDBsingle.sh stop

  ./bin/corfuDBsingle.sh start
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 -v -p 9091 -n 1000 -k 20 -r 0.25 -x -i bnc/initfs_medium.ser -w bnc/wkldfs_medium.ser -h 10 -C CDBLogicalBTree
  ./bin/corfuDBsingle.sh stop

  ./bin/corfuDBsingle.sh start
  ./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A record -t 1 -v -p 9091 -n 10000 -k 20 -r 0.25 -x -i bnc/initfs_large.ser -w bnc/wkldfs_large.ser -h 12 -C CDBLogicalBTree
  ./bin/corfuDBsingle.sh stop

  exit
fi

#for size in tiny small medium large; do
#for class in CDBLogicalBTree CDBPhysicalBTree; do

for size in tiny small; do
for class in CDBLogicalBTree; do

./bin/corfuDBsingle.sh start
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -C $class -A crash -z 20 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i bnc/initfs_$size.ser -w bnc/wkldfs_$size.ser -S > crash-$size-$class.txt
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -C $class -A recover -z 21 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i bnc/initfs_$size.ser -w bnc/wkldfs_$size.ser -S -L crash-$size-$class.txt > recover-$size-$class.txt
./bin/corfuDBsingle.sh stop

cat crash-$size-$class.txt > tmp.txt
cat recover-$size-$class.txt >> tmp.txt
grep TPUT tmp.txt > btree-tput-$size-$class.csv

done
done