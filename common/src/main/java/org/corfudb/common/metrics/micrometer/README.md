### Runtime metrics configuration

*   In order to enable corfu runtime metrics, a logging configuration \*.xml file should be provided with a configured logger.
    Logger name should be "org.corfudb.client.metricsdata".
*   To enable server metrics a logging configuration \*.xml file should be provided with a configured logger. Logger name should be
    "org.corfudb.metricsdata".

### Current metrics collected for LR:

*   **logreplication.message.size.bytes**: Message size in bytes (throughput, mean and max), distinguished by replication type (snapshot, logentry).
*   **logreplication.lock.duration.nanoseconds**: Duration of holding a leadership lock in nanoseconds, distinguished by role (active, standby).
*   **logreplication.lock.acquire.count**: Number of times a leadership lock was acquired, distinguised by role (active, standby).
*   **logreplication.sender.duration.nanoseconds**: Duration of sending a log entry in nanoseconds (throughput, mean and max), distinguished by replication type (snapshot, logentry) and status (success, failure).
*   **logreplication.snapshot.completed.count**: Number of snapshot syncs completed.
*   **logreplication.snapshot.transfer.duration**: Duration of the TRANSFER phase of a snapshot sync in nanoseconds (throughput, mean and max).
*   **logreplication.snapshot.apply.duration**: Duration of the APPLY phase of a snapshot sync in nanoseconds (throughput, mean and max).
*   **logreplication.snapshot.duration**: Duration of completing a snapshot sync in nanoseconds (throughput, mean and max).
*   **logreplication.acks**: Number of acks, distinguished by replication type (snapshot, logentry).
*   **logreplication.messages**: Number of messages sent, distinguished by replication type (snapshot, logentry).
*   **logreplication.opaque.count\_per\_message**: Number of opaque entries per message (rate, mean, max).
*   **logreplication.opaque.count\_total**: Number of overall opaque entries (rate, mean, max).
*   **logreplication.opaque.count\_valid**: Number of valid opaque entries (rate, mean, max).

### Current metrics collected for Corfu Runtime:

*   **runtime.fetch\_layout.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes a client to fetch a layout from Corfu layout servers.
*   **chain\_replication.write**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes a client to write log data (or a hole) into every Corfu logunit server.
*   **open\_tables.count**: Number of currently open tables in the Corfu store.
*   **highestSeqNum.numberBatchReads**: Number of batches read before finding highest DATA sequence number
*   **highestSeqNum.numberReads**: Number of addresses read in batches before finding highest DATA sequence number
*   **highestSequenceNumberDuration**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to complete the latest update.
*   <del>**stream\_sub.delivery.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to deliver a notification to a particular stream listener via a registered callback.</del>
*   <del>**stream\_sub.polling.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to poll the updates of the TX stream for a particular stream listener.</del>
*   <del>**vlo.read.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to access the state of the corfu object backed by a particular stream id.</del>
*   <del>**vlo.write.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to mutate the state of the corfu object backed by a particular stream id.</del>
*   **vlo.tx.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to execute a transaction on the corfu object backed by a particular stream id.
*   **vlo.no\_rollback\_exception.count**: Number of times we were unable to roll back the particular stream by applying undo records in the reverse order.
*   <del>**vlo.sync.rate**: Rate of updates applied/unapplied (mean, max and throughput) to a particular stream, distinguished by a type of update (apply and undo).</del>
*   <del>**vlo.read.rate**: Rate of access to the internal state of the corfu object (mean, max and throughput) backed by a particular stream, distinguished by a type of access (optimistic and pessimistic).</del>
*   **vlo.sync.read\_entries**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of the number of updates of a stream to be sync'd.
*   **vlo.sync.read\_size**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of the total size of the updates of a stream to be sync'd.
*   **address\_space.read\_cache.avg\_entry\_size**: The estimated average size of an entry in the address space cache, in bytes.
*   <del>**address\_space.read\_cache.miss\_ratio**: Ratio of cache read requests which were misses to the Corfu client address space.</del>
*   <del>**address\_space.read\_cache.load\_count**: The total number of times that Corfu client address space cache reads resulted in the load of new values.</del>
*   <del>**address\_space.read\_cache.load\_exception\_count**: The number of times Corfu client address space cache lookups threw an exception while loading a new value.</del>
*   **address\_space.read\_cache.size**: The number of entries in the address space cache.
*   **address\_space.read\_cache.hit\_ratio**: The hit ratio of Corfu client address space cache.
*   <del>**address\_space.read.latency**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes a client to read an object from an address or a range of addresses.</del>
*   <del>**address\_space.write.latency**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes a client to write the given log data using a token.</del>
*   **address\_space.log\_data.size.bytes**: A size estimate distribution in bytes (mean, max, 0.50p, 0.99p) of the log data payload read or written through the address space API.
*   **sequencer.query**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to query the current global tail token in the sequencer or the tails of multiple streams.
*   **sequencer.next**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to get the next token in the sequencer for the particular streams.
*   **sequencer.tx\_resolution**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to acquire a token for a number of streams if there are no transactional conflicts.
*   **sequencer.stream\_address\_range**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to retrieve the address space for the multiple streams.
*   **stream.poll.duration**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to poll transaction updates
*   **stream.notify.duration**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to send notification to client with the pre-registered callback
*   <del>**vlo.sync.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to sync a stream.</del>
*   **stream\_sub.queueDuration.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to wait in the queue.
*   **corfu\_table.read.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to read from the corfu table's map.
*   **corfu\_table.write.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to write to the corfu table's map.
*   **logdata.decompress.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to decompress the payload.
*   **logdata.compress.timer**: Time in microseconds(mean, max, sum, 0.50p, 0.99p) it takes to compress the payload.
*   **logdata.compression.ratio**: A distribution summary (mean, max, 0.50p, 0.99p) of the payload's compression ratio.
*   **logdata.decompressed.size**: A size estimate distribution (mean, max, 0.5p, 0.99p) of the decompressed payload.

### Current metrics collected for Corfu Server:

*   **logunit.write.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes for the stream log to append one logdata or the log data range.
*   **logunit.queue.size**: A size estimate distribution (mean, max, 0.5p, 0.99p) of the number of operations residing in the log unit batch processor.
*   <del>**logunit.read.cache**: Log unit read cache stats (misses, hits, load success and failure counts).</del>
*   **logunit.cache.hit\_ratio**: Ratio of cache read requests which were hits for the log unit server.
*   **logunit.cache.load\_time**: The total number of microseconds the log unit server cache spent loading new values.
*   **logunit.cache.weight**: The sum of weights of evicted log unit server cache entries.
*   **logunit.read.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to read a single address from the stream log (bypassing cache).
*   **logunit.size**: Size of the stream log on disk, measured in open segments, total bytes or number of addresses.
*   **logunit.trimmark**: Current trim mark of the log unit.
*   **logunit.write.throughput**: A distribution summary (mean, max, 0.50p, 0.99p) of payloads (metadata + log entry) measured in bytes written to the stream log.
*   **logunit.read.throughput**: A distribution summary (mean, max, 0.50p, 0.99p) of payloads (metadata + log entry) measured in bytes read from the stream log.
*   **logunit.fsync.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to sync the stream log file to the secondary storage.
*   **sequencer.tx-resolution.timer**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes for the sequencer to check if the TX can commit.
*   **sequencer.tx-resolution.num\_streams**: A distribution summary (mean, max, 0.50p, 0.99p) of the size of the TX conflict set (number of streams), observed by the sequencer server.
*   **sequencer.conflict-keys.size**: A total number of conflict keys in the sequencer cache.
*   **sequencer.cache.evictions**: A distribution summary (mean, max, 0.5p, 0.99p) of the number of evictions per trim call in the sequencer server cache.
*   **sequencer.cache.window**: A sliding window size of the sequencer server cache.
*   **state-transfer.read.throughput**: A size of the state transfer read request size (in terms of number of entries), distinguished by the type of transfer (protocol or committed).
*   **state-transfer.timer**: Time in microseconds (mean, max, 0.5p, 0.99p) it takes to transfer a single batch of addresses (read + write), distinguished by the type of transfer (protocol or committed).
*   **failure-detector.ping-latency**: Time in microseconds (mean, max, 0.50p, 0.99p) it takes for one node in the cluster to ping another node in the cluster.
*   **layout-management-view.consensus**: Time in microseconds (mean, max, 0.50p, 0.99p) it takes for a particular node to reach consensus on a new layout.
*   **corfu.infrastructure.message-handler**\*: Time in microseconds (mean, max, sum, 0.5p, 0.99p) it takes for a particular Corfu server to process the particular incoming RPC.

### Current metrics collected for Table Operations

*   **table.numPuts**: number of put operations
*   **table.numEnqueues**: number of enqueue operations of the CorfuQueue
*   **table.numMerges**: number of merge operations
*   **table.numTouches**: number of touch operations (touch() is a call to create a conflict on a read in a write-only transaction)
*   **table.numDeletes**: number of delete operations
*   **table.numClears**: number of clear operations
*   **table.numGets**: number of get operations
*   **table.numCounts**: number of count operations (count() returns the count of records in the table at a particular timestamp)
*   **table.numKeySets**: number of calling keySet() (returns all keys of the table)
*   **table.numScans**: number of scans
*   **table.numEntryLists**: number of calling entryList() (returns all the Queue entries)
*   **table.numJoins**: number of joins and joined,
*   **table.numGetByIndex**: number of queries by a secondary index
*   **table.numTxnAborts**: number of transaction aborts in this table
*   <del>**openTable**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to open a table</del>
*   **writeTxn**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to do this write-only transaction.
*   **readTxn**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to do this read-only transaction.
*   **readWriteTxn**: Time in microseconds (mean, max, sum, 0.50p, 0.99p) it takes to do this read/write transaction.

### Current metrics collected for Corfu Compactor

*   **checkpoint.timer**: Time in microseconds (mean, max, sum, 0.5p, 0.95p, 0.99p) it takes for a single stream to be checkpointed.
*   **checkpoint.write\_entries**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of the number of entries of a stream that are checkpointed.
*   **checkpoint.write\_size**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of the total size of the entries of a stream that are checkpointed.
