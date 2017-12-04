# Longevity App

The Longevity app is a Corfu application (single Corfu runtime) that spawns a configurable number of threads that will perform CorfuDB operations. While running, the app records all the operations executed along with their result (e.g. Read: map=”mapA”, key=”key1”, value=”value1”, map version = 17).

## Operations
* Execute Snapshot/Optimistic Transaction (random number of Read/Write/Remove operations)
* Read (Normal and Transactional)
* Write (Normal and Transactional)
* Remove (Normal and transactional)
* Checkpoint
* Trim

Checkpoint and Trimming are scheduled at a configurable cyclic interval and the other operations are randomly generated. The key space is configurable with the number of maps and number of keys.

## How to run

usage: longevity
 -c,--corfu_endpoint <arg>   corfu server to connect to
 -cp,--checkpoint            enable checkpoint
 -t,--time_amount <arg>      time amount
 -u,--time_unit <arg>        time unit (s, m, h)

The required arguments are time_amount and time_unit. Corfu endpoint by default is localhost:9000


