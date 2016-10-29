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
