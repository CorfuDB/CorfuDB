#Server Side MVCC Implementation

##Motivation
Currently, clients using CorfuTable have MVCC rollback/sync supported by VersionLockedObject.
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

####Result - Tested on Local Machine
Serialized threads scan with 50 µs average read latency, parallel threads scan in 480 µs average read latency. 

(Thank you to Sundar Sridharan for designing the test.)

###Goals 
We aim to implement MVCC at a server level to attempt to remove the blocking table rollback/sync operations.
Thus, when requests with various states of the table are interleaved, the requests can be served through concurrent
accesses to different versions of the requested objects. This reduces both the overhead of waiting for blocking table
rollback calls as well as the overhead of iteratively applying all stored updates to the base object state. We aim to
trade the network latency arising from querying the 
server for object versions with better concurrency, leading to an increase in throughput. Additionally, the shift to
storing tables server-side allows for client machines to use less memory/storage. 

##Architecture

###Overview
####Current workflow 
Currently, when a CorfuTable is queried, the query is redirected to the VersionLockedObject through a 
CorfuSMRProxy. Any rollback or sync required is performed in the VLO, using the local table in memory to apply updates
to the object state. If an intermediary update or undoRecord is missing, then the VLO queries the server log to receive
all commited updates, and builds up the requested state from the base state of the object. 

####Proposed workflow
We aim to create a new CorfuSMRProxy to bypass the VLO, and instead query a server-side database containing all commited
versions of the requested object. Instead of performing client-side rollback/sync from a copy of the table in memory,
the server side database will be queried for the most recent object state prior to the requested version. By storing
multiple versions of objects in the server side database, clients can concurrently access various versions of the data,
increasing throughput and decreasing client-side memory limitations.

###Storage Engine
We looked into many options to store the CorfuTable. For this use case, we prioritized implementations that provide fast
write times and effecient ranged scans, which are especially important for finding the most recent stored versions for
requested states.

####[RocksDB](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) 
RocksDB is implemented with an LSM-tree, allowing for faster writes. A key feature of this database is Prefix Scan,
where we can specify a prefix extractor to optimize the range scan performance. However, a key downside is that the 
SeekForPrev functionality cannot leverage the prefix bloom filter. To utilize the prefix filter properly, we must
iterate forwards through the key set. RocksDB also does not support multiprocess writes.

####[LMDB](https://github.com/lmdbjava/lmdbjava)
LMBD, in constrast, is implemented with a B+ tree, which is optimized for read performance over write performance.
Similar to RocksDB, LMDB is also an ordered key-value store, allowing for fast range scans through seek and iteration.
Since all data is contained in a single file, there is no prefix search filter optimization like RocksDB. LMDB supports
multiple processes. However, LMDB is restricted to 64-bit systems and lock-aware file systems, and has no automatic
compaction.

####[Tkrzw](https://dbmx.net/tkrzw/)
Tkrzw provides many different implementations of DBM, including implementations based on B+ trees. These implementations
(TreeDBM and BabyDBM) are ordered, allowing for iteration over keys, and performing seeks to the latest version of a
record. These DBMs can work with both String keys, and byte arrays. However, Tkrzw is not an open source project, and
the dependency is not available on Maven central.

After considering the options, we have decided to utilize RocksDB due to its write-efficiency, configurability, and
support for essential features.

