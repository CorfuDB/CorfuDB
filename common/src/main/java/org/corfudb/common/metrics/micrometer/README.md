### Runtime metrics configuration

* In order to enable corfu runtime metrics, a logging configuration *.xml file should be provided with a configured logger. 
Logger name should be "org.corfudb.client.metricsdata".
* To enable server metrics a logging configuration *.xml file should be provided with a configured logger. Logger name should be
"org.corfudb.metricsdata".

### Current metrics collected for LR:

  * **logreplication.message.size.bytes**: Message size in bytes (throughput, mean and max), distinguished by replication type (snapshot, logentry).
  * **logreplication.lock.duration**: Duration of holding a leadership lock in seconds, distinguished by role (active, standby).
  * **logreplication.lock.acquire.count**: Number of times a leadership lock was acquired, distinguised by role (active, standby).
  * **logreplication.sender.duration.seconds**: Duration of sending a log entry in seconds (throughput, mean and max), distinguished by replication type (snapshot, logentry) and status (success, failure).
  * **logreplication.rtt.seconds**: Duration of sending a message overall (throughput, mean and max).
  * **logreplication.snapshot.completed.count**: Number of snapshot syncs completed.
  * **logreplication.snapshot.duration**: Duration of completing a snapshot sync in seconds (throughput, mean and max).
  * **logreplication.acks**: Number of acks, distinguished by replication type (snapshot, logentry).
  * **logreplication.messages**: Number of messages sent, distinguished by replication type (snapshot, logentry).
  * **logreplication.opaque.count_valid**: Number of opaque entries per message (rate, mean, max).
  * **logreplication.opaque.count_total**: Number of overall opaque entries (rate, mean, max).
  * **logreplication.opaque.count_valid**: Number of valid opaque entries (rate, mean, max).

### Current metrics collected for Corfu Runtime:

* **runtime.fetch_layout.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to fetch a layout from Corfu layout servers.
* **chain_replication.write**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to write log data (or a hole) into every Corfu logunit server.
* **open_tables.count**: Number of currently open tables in the Corfu store.
* **stream_sub.delivery.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to deliver a notification to a particular stream listener via a registered callback. 
* **stream_sub.polling.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to poll the updates of the TX stream for a particular stream listener. 
* **vlo.read.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to access the state of the corfu object backed by a particular stream id. 
* **vlo.write.timer**: Time in milliseconds (mean, max, sum, 0.5 F0p, 0.95p, 0.99p) it takes to mutate the state of the corfu object backed by a particular stream id.
* **vlo.tx.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to execute a transaction on the corfu object backed by a particular stream id.
* **vlo.no_rollback_exception.count**: Number of times we were unable to roll back the particular stream by applying undo records in the reverse order.
* **vlo.sync.rate**: Rate of updates applied/unapplied (mean, max and throughput) to a particular stream, distinguished by a type of update (apply and undo).
* **vlo.read.rate**: Rate of access to the internal state of the corfu object (mean, max and throughput) backed by a particular stream, distinguished by a type of access (optimistic and pessimistic).
* **address_space.read_cache.miss_ratio**: Ratio of cache read requests which were misses to the Corfu client address space.
* **address_space.read_cache.load_count**: The total number of times that Corfu client address space cache reads resulted in the load of new values.
* **address_space.read_cache.load_exception_count**: The number of times Corfu client address space cache lookups threw an exception while loading a new value.
* **address_space.read.latency**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to read an object from an address or a range of addresses.
* **address_space.write.latency**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to write the given log data using a token.
* **address_space.log_data.size.bytes**: A size estimate distribution in bytes (mean, max, 0.50p, 0.95p, 0.99p) of the log data payload read or written through the address space API.
* **sequencer.query**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to query the current global tail token in the sequencer or the tails of multiple streams.
* **sequencer.next**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to get the next token in the sequencer for the particular streams.
* **sequencer.tx_resolution**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to acquire a token for a number of streams if there are no transactional conflicts.
* **sequencer.stream_address_range**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to retrieve the address space for the multiple streams.

### Current metrics collected for Corfu Server:

* **logunit.write.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes for the stream log to append one logdata or the log data range.
* **logunit.queue.size**: A size estimate distribution (mean, max, 0.5p, 0.95p, 0.99p) of the number of operations residing in the log unit batch processor.
* **logunit.read.cache**: Log unit read cache stats (misses, hits, load success and failure counts).
* **logunit.cache.hit_ratio**: Ratio of cache read requests which were hits for the log unit server.
* **logunit.cache.load_time**: The total number of milliseconds the log unit server cache spent loading new values.
* **logunit.cache.weight**: The sum of weights of evicted log unit server cache entries.
* **logunit.read.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to read a single address from the stream log (bypassing cache).
* **logunit.size**: Size of the stream log on disk, measured in open segments, total bytes or number of addresses.
* **logunit.trimmark**: Current trim mark of the log unit.
* **logunit.write.throughput**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of payloads (metadata + log entry) measured in bytes written to the stream log.
* **logunit.read.throughput**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of payloads (metadata + log entry) measured in bytes read from the stream log.
* **logunit.fsync.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to sync the stream log file to the secondary storage.
* **sequencer.tx-resolution.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes for the sequencer to check if the TX can commit. 
* **sequencer.tx-resolution.num_streams**: A distribution summary (mean, max, 0.50p, 0.95p, 0.99p) of the size of the TX conflict set (number of streams), observed by the sequencer server.
* **sequencer.conflict-keys.size**: A total number of conflict keys in the sequencer cache. 
* **sequencer.cache.evictions**: A distribution summary (mean, max, 0.5p, 0.95p, 0.99p) of the number of evictions per trim call in the sequencer server cache. 
* **sequencer.cache.window**: A sliding window size of the sequencer server cache.
* **state-transfer.read.throughput**: A size of the state transfer read request size (in terms of number of entries), distinguished by the type of transfer (protocol or committed).
* **state-transfer.timer**: Time in milliseconds (mean, max, 0.5p, 0.95p, 0.99p) it takes to transfer a single batch of addresses (read + write), distinguished by the type of transfer (protocol or committed).
* **failure-detector.ping-latency**: Time in milliseconds (mean, max, 0.50p, 0.95p, 0.99p) it takes for one node in the cluster to ping another node in the cluster. 
* **layout-management-view.consensus**: Time in milliseconds (mean, max, 0.50p, 0.95p, 0.99p) it takes for a particular node to reach consensus on a new layout.
* **corfu.infrastructure.message-handler***: Time in milliseconds (mean, max, sum, 0.5p, 0.95p, 0.99p) it takes for a particular Corfu server to process the particular incoming RPC.



