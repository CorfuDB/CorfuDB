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
