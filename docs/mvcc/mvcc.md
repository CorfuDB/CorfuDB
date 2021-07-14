#Server Side MVCC Implementation

##Motivation
Currently, clients using CorfuTable have version rollback/sync supported by VersionLockedObject.
However, when querying a key at a certain version, the whole table must be rolled back to that version, 
requiring blocking calls.

Additionally, since all tables are stored in memory, user may run into memory issues while trying to run multiple
clients from the same machine.

###Motivating Example
####Test in VLO Table 
1. Insert 100 documents
2. Take a snapshot (Snapshot A)
3. Insert 100 more documents 
4. Take a snapshot (Snapshot B)
5. Thread 1 iterates over Snapshot A 
6. Thread 2 iterates over Snapshot B

####Result
Serialized threads scan in 120 ms, parallel threads scan in 960 ms. 

(Thank you to Sundar Sridharan for designing and conducting the test.)

###Goals 
We aim to implement MVCC at a server level to attempt to remove the blocking table rollback/sync operations.
Thus, when requests with various states of the table are interleaved, the requests can be served through concurrent
accesses to different versions of the requested objects. This reduces both the overhead of waiting for blocking table
rollback calls as well as the overhead of iteratively applying all stored updates to the base object state. We aim to
trade the network latency arising from querying the 
server for object versions with better concurrency, leading to an increase in throughput. Additionally, the shift to
storing tables server-side allows for client machines to use less memory/storage. 

##Architecture

###Storage Engine
We looked into many options to store the CorfuTable. For this use case, we prioritized implementations that provide fast
write times and effecient ranged scans, which are especially important for finding the most recent stored versions for
requested states.

####[RocksDB](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) 
RocksDB is implemented with an LSM-tree, allowing for faster writes. A key feature of this database is Prefix Scan,
where we can specify a prefix extractor to optimize the range scan performance. However, a key downside is that the 
SeekForPrev functionality cannot leverage the prefix bloom filter. To utilize the prefix filter properly, we must
iterate forwards through the key set.