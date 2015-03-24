#!/bin/bash
(set -o igncr) 2>/dev/null && set -o igncr; # this comment is required
# Above line makes this script ignore Windows line endings, to avoid this error without having to run dos2unix:
#   $'\r': command not found
# As per http://stackoverflow.com/questions/14598753/running-bash-script-in-cygwin-on-windows-7

# btreetest.sh [--iter i] [--verbose] [--type t] [--masternode m] [--help]

./bin/corfuDBsingle.sh stop
./bin/corfuDBsingle.sh start
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A crash -z 20 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i initfs_25.ser -w wkldfs_25.ser > crash.txt
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A recover -z 20 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i initfs_25.ser -w wkldfs_25.ser -L crash.txt > recover.txt
./bin/corfuDBsingle.sh stop

cat crash.txt > tmp.txt
cat recover.txt >> tmp.txt
grep TPUT tmp.txt > btree-tput-coarse.csv

./bin/corfuDBsingle.sh start
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A crash -z 20 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i initfs_25.ser -w wkldfs_25.ser -C CDBPhysicalBTree > crash.txt
./bin/corfuDBTestRuntime.sh CorfuDBTester -m http://localhost:8002/corfu -A recover -z 20 -t 1 -n 20 -k 20 -r 0.25 -v -x -p 9091 -i initfs_25.ser -w wkldfs_25.ser -L crash.txt -C CDBPhysicalBTree > recover.txt
./bin/corfuDBsingle.sh stop

cat crash.txt > tmp.txt
cat recover.txt >> tmp.txt
grep TPUT tmp.txt > btree-tput-fine.csv
