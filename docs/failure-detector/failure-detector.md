## _Failure detector_

Main component in the failure detector is RemoteMonitoringService.
The scheduler starts RemoteMonitoringService every 3 seconds (by default).

Failure detector component (RemoteMonitoringService) consists of 4 parts: 
 - collect poll report
 - correct wrong epochs
 - healing process
 - failure detector
 
### Collect poll report
 - Each node:
      - pings other nodes 3 times and build local NodeState. 
      - collects wrong epochs.
      - collects remote NodeSate's from other nodes.
 - Aggregate ClusterState from the previous step.     

### Wrong epoch detector
 - if there is at least one wrong epoch in the cluster, then trigger the correction process. 
 - check that the slot is unfilled. If the slot is unfilled then fill the slot.
   Unfilled slot means that a new epoch was created but there is no a layout for that epoch. 
   The layout must must be updated to a new epoch.
 - if there was at least one `wrong epoch` response then finish current iteration of a failure detection

### Healing process
 - Build a ClusterGraph based on the aggregated ClusterState (the cluster state built on the poll report stage)
 - check if local node is in the unresponsive list.
 - figure out if we can heal our self:
   1. the node has to be fully connected which means it has to be connected to all responsive nodes in the cluster.
 - heal our self if the node is fully connected.
 
### Failure detector
 ![Failure detector visualization](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/CorfuDB/CorfuDB/master/docs/failure-detector/failure-detector.puml)

 - Corfu cluster consist of three nodes: `[a, b, c]`.
 - There is a network failure between node `b` and node `c`.
 - Build a ClusterGraph based on aggregated ClusterState (the cluster state built on poll report stage):
   - find a failure in the cluster graph (if exists).
   - get a `Decision maker` - the node which can add a node to the unresponsive list. The decision maker node in the cluster:
     1. has highest number of successful connections in the cluster (there could be more than one decision maker in a cluster).
     2. has lowest node name in the cluster. For instance: [a, b, c] - `a` has lowest name, `c` has highest name.
   - choose a failed node:
     1. the node has lowest number of successful connections.
     2. the node has highest node name in the cluster.
   - if local node is a decision maker then the node adds the failed node in the unresponsive list.
   
In the example:
 - node `a` is a decision maker, it has 3 successful connections
 - node `c` is a failed node, it has same number of successful connections like node `b` but is has higher node name.
 - node `a` will add node `c` to the unresponsive list.     
 