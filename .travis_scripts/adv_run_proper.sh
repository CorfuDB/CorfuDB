#!/bin/bash

# set -x

echo '## Use TravisCI build of Erlang 17.5'
mkdir -p $HOME/otp
(
    cd $HOME/otp
    wget https://s3.amazonaws.com/travis-otp-releases/ubuntu/12.04/erlang-17.5-x86_64.tar.bz2
    tar xf $HOME/otp/erlang-17.5-x86_64.tar.bz2 -C $HOME/otp
)
echo '## Does Erlang work?'
. $HOME/otp/17.5/activate

which erl
erl  -eval '{io:format(user, "~p\n", [catch orddict:is_empty([])]), timer:sleep(1000), erlang:halt(0)}.'

echo '## Clone & build PropEr.'
(
    cd $HOME/otp
    git clone git://github.com/manopapad/proper.git
    cd proper
    export PROPER_DIR=`pwd`
    make
)
export PROPER_DIR=$HOME/otp/proper

echo '## Install Expect to deal with the pty expected by the Clojure shell'
sudo apt-get -y install expect

echo '## Stop all java processes.'
killall java ; sleep 1 ; killall -9 java
sleep 1

echo '## Start epmd'
erl -noshell -sname testing -s erlang halt

echo '## Start Corfu server'
data_dir=/tmp/some/path
rm -rf $data_dir ; mkdir -p $data_dir
log_file=$data_dir/server-log.out
touch $log_file
cat<<EOF > $data_dir/run
#!/usr/bin/expect

spawn bin/shell

send "(def q-opts (new java.util.HashMap))\n"
send "(.put q-opts \"<port>\" \"8000\")\n"
send "(new org.corfudb.cmdlets.QuickCheckMode q-opts)\n"
send "(org.corfudb.infrastructure.CorfuServer/main (into-array String (.split \"-l /tmp/some/path -s -d WARN 8000\" \" \")))\n"

set timeout 120
while {1} {
    expect -re {
        timeout { tell "\n\nTIMEOUT\n" ; exit 0 }
        eof     { tell "\n\nEOF\n" ; exit 0 }
        .       { }
    }
}

EOF
chmod +x $data_dir/run
ps axww | grep epmd
$data_dir/run > $log_file 2>&1 &
sleep 1

echo '## Wait for Corfu server to be alive ... ' `date`
count=0
# set +x
while [ $count -lt 30 ]; do
    if [ `grep "Sequencer recovery requested" $log_file | wc -l` -ne 0 ]; then
        break
    fi
    sleep 1
    count=`expr $count + 1`
done
echo '## Done waiting ... ' `date`

# set -x

echo '## Build PropEr'
cd test/src/test/erlang
./Build.sh proper


echo '## Run QuickCheck tests'
errors=0
set -x

/usr/bin/time ./Build.sh proper-shell -noshell -s map_qc cmd_prop
errors=`expr $errors + $?`

/usr/bin/time ./Build.sh proper-shell -noshell -s map_qc cmd_prop_parallel
errors=`expr $errors + $?`

set +x

echo '## Stop server processes'
killall java ; sleep 1 ; killall -9 java
killall epmd ; sleep 1 ; killall -9 epmd

echo '## Report result (stdout, exit status)'

# egrep 'ERR|WARN' $log_file | egrep -v 'Sequencer recovery requested but checkpoint not set'
cat $log_file | \
    egrep -v 'Sequencer recovery requested but checkpoint not set' | \
    sed '/expect: spawn id/,$d'

echo '## PropEr test final exit status:' $errors
exit $errors
