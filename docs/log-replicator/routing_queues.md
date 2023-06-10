## Log Replication via Routing Queues & Destination stream tags

[Primary Requirements](#primary-requirements)

[Routing Queues & Stream Tags](#routing-queues-and-destination-stream-tags)

[LR Client on Sender](#client-on-sender)

[LR Server on Sender](#log-entry-reader-in-server-on-sender)

[LR Full Snapshot Sync Sender](#full-sync-sender)

[LR Server on Receiver](#lr-server-on-sink)

[LR Client on Receiver](#lr-client-on-sink)

### Primary requirements
When a transaction touches a record on the source cluster that mutation
needs to be disseminated to one or more remote sites.
The data also needs to be modified after dissemination on the sink cluster.

Client will provide one or more opaque payloads with a list of destinations in a transaction.
LR client on the sender intercepts this in the same transaction, transforms & persists this in corfu.

**If a payload needs to go to multiple sites, that payload must not be duplicated since there can be
several sites.**
The payload is opaque to LR, the receiver is the one that needs to read this data and apply the updates to various tables.

### Routing Queues and Destination Stream tags
LR Server on source and sink will have one session per remote destination.
Data from the clients will be written into the following well known Routing Queues
1. `LRQ_Send_LogEntries`: This is the one shared queue on the sender where delta updates are placed transactionally.
   1. Entries here are not checkpointed and are lost after a trim cycle.
2. `LRQ_Send_SnapSync`: This is the one shared queue on the sender where full snapshot sync updates are placed.
   1. Entries in this queue are not checkpointed and data loss occurs after a trim cycle.
3. `LRQ_Recv_<remote_id>`: This is the queue where the LR on Sink places the updates arriving from the remote source.
   1. Entries in this queue are checkpointed normally.

Each update made carries destinations and is also tagged with a destination specific stream tag with the following convention:
1. `lrq_logentry_<remote_id>`: Destination specific stream tag applied when entries are placed into `LRQ_Send_LogEntries`
2. `lrq_snapsync_<remote_id>`: Destination specific stream tag applied when entries are placed into `LRQ_Send_SnapSync`
3. `lrq_recv`: This is the tag applied on the sink by LR server to all updates be it full sync or log entry sync.

### Client on sender
The main api intercepting the transaction is something like
```java
class LRoutingQueueClient {
    public boolean transmitLogEntryMessage(byte[] payload, List<String> destinations);
    public boolean transmitLogEntryMessages(List<LRMessageWithDestinations> messages); // optional?
}

class LRMessageWithDestinations {
    public byte[] payload;
    public List<String> destinations;
}
```
The first api to be called as many times as needed within the same transaction
where the application is persisting data.

Under the hood this api translates to a single `logUpdate(UUID streamId, SMREntry updateEntry, List<UUID> streamTags);`
to the new shared `LRQ_Send_LogEntries` as follows

```protobuf
// prepare this to look like a CorfuQueue update
message RoutingTableEntryMsg {
  repeated string destinations = 1;
  required bool is_snapshot_end = 2;
  required bytes opaque_payload = 3;
}
```

1. `transmitLogEntryMessage()` just does one `logUpdate()` to the `LRQ_Send_LogEntries` stream per call
adding one stream tag per destination, completely bypassing the Object Layer.
This is done to save memory, it does mean that LR at this point will not be able to apply backpressure using techniques like number of outstanding entries in this queue.
2.Each `logUpdate()` of this update will need look exactly like a CorfuQueue's enqueue(RoutingTableEntryMsg) operation.
   1. This is done for 2 reasons - the queue's id captures the sequencer's address which can later be used for negotiation
   2. Tools like browser can be used to debug and display outstanding entries.
   3. OPTIONAL:Update the queue id generated for that destination in a normal LR metadata table per destination, so that this last seen queue id can be used to compare against a snapshot sync negotiation request & avoid a full sync

### Log Entry Reader In Server on Sender
The new Routing Queue based Log Entry reader is started per discovered destination and does the following:
1. Listens on the destination specific stream tag.
2. Reads the logUpdate of the global shared `LRQ_Send_LogEntries` tagged with its destination tag
3. Deserializes all Queue entry (RoutingTableEntryMsg) -> puts all these in an ArrayList specific only to this update or transaction.
4. Search for my specific destination and get its payload
5. Remove the destinations field, just replicate the queue without the destination
6. Change the stream id in the opaque entry as `LRQ_Recv_<my_source_id>` because on the sink we only want one queue for both full snapshot sync and log entry sync.
7. Applies the stream tag `lrq_recv`. This is the one stream tag the listener on the sink will subscribe to.
8. replicates this & then the current read entry can be garbage collected

```java
/**
 // Optional optimization to avoid searching if we are always given all the payloads together
logUpdate("UUID_OF_ROUTING_TABLE", (PayloadIdentifier -> PayloadInBytes
"0"-> DESTINATION1->payloadId_1, DESTINATION2->payloadOffset1, DESTINATION3->payloadOffset2, Destinatio4->payloadOffset2)
"1"->PAYLOAD_A,
"2"->PAYLOAD_B)
tag_for_destination1, tag_for_destination2...
)
 */
```

## Full Sync Sender
1. `LRQ_Send_SnapSync` is the shared stream to which the full sync data is written to as queue.
2. LR Server requests a full sync and puts a record in its internal metadata table.
3. LR Client listens to this and requests a full sync via callback.
4. The supplied data is placed in the above queue.
5. LR Server which is listening for updates to this `LRQ_Send_SnapSync` wakes up and starts transmitting.
6. Before transmitting the stream is changed to `LRQ_Recv_<my_cluster_id>` so it can be applied to one single queue on receiver.
7. Also the stream tags are stripped and only one tag is applied - `lrq_recv`
8. LR Client places an end marker `is_snapshot_end`.
9. LR Server observes this end marker and concludes the snapshot sync and transitions state machine to log entry sync.
10. We only need 1 queue for the receiver/Sink.

## LR Server on Sink
Minimal to no changes here - incoming Queue data is simply applied into the `LRQ_Recv_<remote_source_id>`.
**This allows models where many remote source sites are replicating to a single sink cluster to be distinguished
from one another**

## LR Client on Sink
Similar to the ReplicationGroup routing model, listens to both the ReplicationStatus table and the routing queue.
Identifies when a full sync as started and delivers full sync messages.
If there are log entry message, these are also delivered subsequently.
It is up to the client to read the envelope information and apply its contents to the respective tables.