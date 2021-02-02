### Runtime metrics configuration

* In order to enable corfu runtime metrics, a logging configuration *.xml file should be provided with a configured logger. 
Logger name should be "org.corfudb.client.metricsdata".
* To enable server metrics a logging configuration *.xml file should be provided with a configured logger. Logger name should be
"org.corfudb.metricsdata".

### Current metrics collected for LR:

  * Message size in bytes (throughput, mean and max), distinguished by replication type (snapshot, logentry): **logreplication.message.size.bytes**.
  * Duration of holding a leadership lock in seconds, distinguished by role (active, standby): **logreplication.lock.duration**.
  * Number of times a leadership lock was acquired, distinguised by role (active, standby): **logreplication.lock.acquire.count**.
  * Duration of sending a logentry in seconds (throughput, mean and max), distinguished by replication type (snapshot, logentry) and status (success, failure): **logreplication.sender.duration.seconds**.
  * Duration of sending a message overall (throughput, mean and max): **logreplication.rtt.seconds**.
  * Number of snapshot syncs completed. **logreplication.snapshot.completed.count**.
  * Duration of completing a snapshot sync in seconds (throughput, mean and max): **logreplication.snapshot.duration**.
  * Number of acks, distinguished by replication type (snapshot, logentry): **logreplication.acks**.
  * Number of messages sent, distinguished by replication type (snapshot, logentry): **logreplication.messages**.
  * Number of opaque entries per message (rate, mean, max): **logreplication.opaque.count_valid**.
  * Number of overall opaque entries (rate, mean, max): **logreplication.opaque.count_total**.
  * Number of valid opaque entries (rate, mean, max): **logreplication.opaque.count_valid**.

### Current metrics collected for Corfu Runtime:

* **runtime.fetch_layout.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to fetch a layout from Corfu layout servers.
* **chain_replication.write**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to write log data (or a hole) into every Corfu logunit server.
* **open_tables.count**: Number of currently open tables in the CorfuStore.
* **stream_sub.delivery.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p), it takes to deliver a notification to a particular stream listener via a registered callback. 
* **stream_sub.polling.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p), it takes to poll the updates of the TX stream for a particular stream listener. 
* **vlo.read.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p), it takes to access the state of the corfu object backed by a particular stream id. 
* **vlo.write.timer**: Time in milliseconds (mean, max, sum, 0.5 F0p, 0.95p, 0.99p), it takes to mutate the state of the corfu object backed by a particular stream id.
* **vlo.tx.timer**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p), it takes to execute a transaction on the corfu object backed by a particular stream id.
* **vlo.no_rollback_exception.count**: Number of times we were unable to roll back the particular stream by applying undo records in the reverse order.
* **vlo.sync.rate**: Rate of updates applied/unapplied (mean, max and throughput) to a particular stream, distinguished by type of update (apply and undo).
* **vlo.read.rate**: Rate of access to the internal state of the corfu object (mean, max and throughput) backed by a particular stream, distinguished by a type of access (optimistic and pessimistic).
* **address_space.read_cache.miss_ratio**: Ratio of cache read requests which were misses to the Corfu client address space.
* **address_space.read_cache.load_count**: The total number of times that Corfu client address space cache reads attempted to load new values.
* **address_space.read_cache.load_exception_count**: The number of times Corfu client address space cache lookups threw an exception while loading a new value.
* **address_space.read.latency**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to read an object from an address or a range of addresses.
* **address_space.write.latency**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes a client to write the given log data using a token.
* **address_space.log_data.size.bytes**: A size estimate distribution in bytes (mean, max, 0.50p, 0.95p, 0.99p) of the log data payload read or written through the address space API.
* **sequencer.query**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to query the current global tail token in the sequencer or the tails of multiple streams.
* **sequencer.next**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to get the next token in the sequencer for the particular streams.
* **sequencer.tx_resolution**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to acquire a token for a number of streams if there are no transactional conflicts.
* **sequencer.stream_address_range**: Time in milliseconds (mean, max, sum, 0.50p, 0.95p, 0.99p) it takes to retrieve the address space for the multiple streams.



