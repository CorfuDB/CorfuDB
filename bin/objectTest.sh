#!/bin/bash

for txcount in 1 2 3 4 5 6 7 8 9 10; do
    #bin/corfuDBTest.sh ObjectClient --txns $txcount --runs 10 --numObjs 10 > data/2PL.${txcount}.10objs.data
    bin/corfuDBTest.sh ObjectClient --txns $txcount --runs 10 --numObjs 10 --test MDCC > data/MDCC.${txcount}.10objs.data
done
