#!/bin/bash

set -x

whoami
ls -al ~
cat ~/.kerlrc
which kerl
kerl list releases
kerl list builds
kerl list installations

which erl
ls -l ~/.kerl/builds/17.5/
ls -l ~/.kerl/builds/

kerl install 17.5 ~/e
. ~/e/activate
which erl
erl -s erlang halt

## Alright, try TravisCI plan B.
mkdir ~/otp
cd ~/otp
wget https://s3.amazonaws.com/travis-otp-releases/ubuntu/12.04/erlang-17.5-x86_64.tar.bz2
tar xf ~/otp/erlang-17.5-x86_64.tar.bz2 -C ~/otp
. ~/otp/17.5/activate
which erl
erl -s erlang halt

cd ~/otp
git clone git://github.com/manopapad/proper.git
cd proper
make
