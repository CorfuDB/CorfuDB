## _Local Node Failure Detection Design_

Failure detection mechanism, briefly:
 - Each node in a cluster provides NodeState information to the rest of the nodes in the cluster.
 - Each node collects NodeState-s from all nodes in a cluster. ClusterState is an aggregated state of all nodes in the cluster.
 - The healthiest node in the cluster (with the highest number of successful connections) 
   is a 'decision maker' node, which decides what node should be excluded from the cluster.

To handle failures with better quality a failure detection mechanism can be improved in a following way.
NodeState will contain not only connection status but also other information regarding node's health status.
Having that information in NodeState enables Failure Detector to add an unhealthy node to the unresponsive list.

### _Types of a Local Node Failures_

File System (LogUnit) Failures:
- ResourceQuota limits exceeded
- Partition:
    - read only partition
    - check if a partition mounted
    - disk latency
- DataCorruption error

JVM failures:
- JVM GC failures (app stops)
- OOM error

System Failures:
- No free RAM memory

### Detecting Failures

Each node runs its own instance of a [Failure Detector](failure-detector.md).
Since each node will provide all needed information in its NodeState, a decision maker can detect any possible failure 
in a node state like QuotaExceeded or a connection failure or read only partition and so on. 
After that the decision maker just marks the node as unresponsive.

Providing information about node status instead of trying to mark itself as unresponsive could/should be more beneficial. 
For instance, there are multiple possible scenarios of failures that could happen:
 - _a local node_ can't be functioning properly, because of a partition is mounted in read only mode. 
   Since we can't write anything into corfu's db directory, there is no way to correct/update a layout to the latest one,
   no way to correct wrong epoch, or do anything that requires writing to the database directory. 
   In that case, providing information to a "decision maker" node will allow the node 
   add failed node to the unresponsive list and update current layout in the cluster.
 - _Preventing domino effect and gradual and graceful degradation of a cluster_ - if quota exceeds on all nodes at the same time
   then each node independently will try to add itself into the unresponsive list, which causes domino effect - many
   simultaneous updates of the cluster layout. Using current "decision maker" approach only one node can be added to 
   the unresponsive list at a time.
 
#### Local Node Failure Detection Sequence Diagram

![Local Node Failure Detection Visualization](https://raw.githubusercontent.com/CorfuDB/CorfuDB/failure-detector-read-only-filesystem/docs/failure-detector/file-system-failure-detection.puml)