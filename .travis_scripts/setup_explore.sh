#!/bin/bash

set -x

whoami
ls -al ~
cat ~/.kerlrc
which kerl
kerl list releases
kerl list builds
kerl list installations

kerl install 17.5 ~/e
. ~/e/activate
erl -s erlang halt
