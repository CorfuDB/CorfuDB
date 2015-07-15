#!/usr/bin/env bash
set -e
if [ ! -d "$HOME/protobuf/lib" ]; then
    wget http://apache.mirrors.tds.net/thrift/0.9.2/thrift-0.9.2.tar.gz -O thrift.tar.gz;
    tar -xzf thrift.tar.gz;
    cd thrift-0.9.2 && ./configure JAVA_PREFIX=$HOME/thrift --prefix=$HOME/thrift --without-ruby --without-python --without-c --without-erlang --without-go --without-nodejs && make && make install && cd  ..;
else
    echo "Using cached thrift";
fi
