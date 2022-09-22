Log Replicator(LR) allows Corfu log-based replication between multiple Corfu
clusters.

This replication is selective, i.e., clients/applications can select the streams
to replicate.

To be considered for replication, the protobuf schema of the payload of a stream
must have the 'is_federated' option set to true.
For example:
message IntValueTag {
    option (org.corfudb.runtime.table_schema).stream_tag = "test";
    option (org.corfudb.runtime.table_schema).is_federated = true;
    int32 value = 2;
}

LR reads the Registry Table and filters all streams with this flag marked as
true.  The list(in-memory) is first constructed during initialization of LR and
later updated as new tables get opened and the Registry Table is updated with their
info.  On the Sink cluster(receiver), it is also updated with the replicated
data from the Source(sender) unless it conflicts with its own information.  More
on this conflict is explained in the Special Cases section.

=============
Requirements
=============
1. The set of streams to replicate will be updated according to the above flag
   as tables get opened and are discovered through replication on the Sink.
2. This set will be consistent across the Source and Sink except for cases
   described in the Special Cases section.

=========
Behavior
=========

Source
--------
After initialization, the Source cluster will update its set of streams to
replicate by syncing it with the Registry Table at the start and end of every
LogEntry and Snapshot Sync.

Additionally, on LogEntry Sync, whenever a new stream to replicate is detected,
the in-memory set maintained by LR will be updated.

NOTE: Any new tables added/opened on the Source with is_federated=true when
Snapshot sync is ongoing will NOT be included in the current cycle of Snapshot sync.

Sink
------
After initialization, the Sink cluster will update its set of streams to
replicate by syncing it with the Registry Table at the start and end of every
LogEntry and Snapshot Sync.

On LogEntry and Snapshot Sync, whenever a new stream is received from the Source,
it will get added to the Sink LR's set of streams to replicate unless it conflicts
with its own information.  Please refer to the section on Special Cases for more
details on the conflict.

Both Source and Sink
---------------------
LR's in-memory set of streams to replicate will be synced with the Registry
Table whenever there is a leadership change.


==============
Special Cases
==============
The set of streams to replicate should ideally be consistent on both the Source
and Sink clusters.  However, if a stream has been opened on each cluster with a
diffferent value of 'is_federated' flag, this difference will be honoured.

For example, if streamA is to be replicated as per its metadata on Source but
not as per its metadata on the Sink, it will be dropped on the Sink.

A valid case of this is during an upgrade cycle where one cluster has
upgraded(the supported workflow is to upgrade the Sink followed by the Source).
The same stream can have different metadata(is_federated flag) on each version.
If the stream is not marked for replication on the Sink, its received data will
not be applied.  Its metadata in the Registry Table will also not be overwritten.

In case of a user error or incorrect usage in applications, a stream can be
opened with different metadata on each cluster even if both are on the same
version.  This is a user error and cannot be detected in LR.  This case will
also be handled in the same way as the upgrade case.

Also note that in both cases, the detection of conflicting metadata on the Sink
is possible only if the stream has been locally opened(present in the
Registry Table).  If it has not been opened, the replicated data from the Source
will be applied.
