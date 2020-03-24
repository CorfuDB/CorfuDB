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
 
### Implementation

**Remote Monitoring Service** comprises of monitoring, failure handling and healing.
This service is responsible for heartbeat and aggregating the cluster view. 
This is updated in the shared context with the management server which serves the heartbeat responses.
The failure detector updates the unreachable nodes in the layout.
The healing detector heals nodes which were previously marked unresponsive but have now healed.


**Collect poll report**
Each poll round consists of iterations. In each iteration, the FailureDetector pings all responsive nodes 
in the layout and also collects their node states to provide cluster state.
To provide more resilient failre detection mechanism, the failure detector collects few "poll reports" 
equal to `failureThreshold` number then aggregates final poll report

Once a poll report collected, the `failureDetectionTask` is triggered by Remote Monitoring Service.
To detect a failure in the cluster the Remote Monitoring Service gets `cluster state` from a poll report.
The cluster state contains information about the cluster.

**ClusterAdvisor** detects failures using cluster state. There could be different types of cluster advisors.
Currently we use `CompleteGraphAdvisor` - any alive node must be connected to all other alive nodes in the cluster.
`CompleteGraphAdvisor`:
 - The advisor transforms cluster state into the cluster graph and makes it symmetric.
 It means converting a cluster graph which could have asymmetric failures to a graph with symmetric failures between nodes.
 For instance: <br/>
 {a: [{a: OK, b: FAILED}]} <br/>
 {b: [{a: OK, b: OK}]} <br/>
 Node A believes that node B is disconnected <br/>
 Node B believes that node A is connected <br/>
 The graph will be changed to: <br/>
 {a: [{a: OK, b: FAILED}]} <br/>
 {b: [{a: FAILED, b: OK}]} <br/>
 Node B is not connected to node A anymore.
 
**Decision maker** - is a node which can decide to add a failed node into the unresponsive list.
The decision maker is an technique, used to reduce parallel updates of the cluster state.
Still, each node can choose its own decision maker.
Note: it's not required to choose a particular one for entire cluster to add a node to unresponsive list.

The decision maker must have:
- highest number of successful connections in the graph.
There could be many nodes with same number of successful connections, 
then the decision maker will be a node with smallest name.

We also have additional checks to prevent all possible incorrect ways. Decision maker can't be found if:
- `ClusterGraph` is empty, which is an invalid state.
- the decision maker doesn't have connections, which is also impossible.

Detecting **failed node** - find a node in the cluster with a minimum number of successful connections.
To find failed nodes and heal nodes in the cluster we use `NodeRank`-s.

`NodeRank` is the rank of the node in a graph. Nodes sorted according to their ranks. 
Sorting nodes according to:
- Descending order of number of connections
- Ascending order of a node name (alphabetically)

A `Decision maker` always has highest number of successful connections and smallest name and
a failed node always has lowest number of successful connections and highest name.

Finding a failed node - collect all node ranks in a graph and choose the node 
with the smallest number of successful connections and with the highest name (sorted alphabetically).

Even if we have found a node with smallest number of connections it doesnt mean that the node is 
failed. We also have to check if the node is *fully connected*. 
Only if the node is not fully connected and has the smallest number of successful connections then
the node can be added to the unresponsive list.   

**Healing process** - determine if a node is alive and remove it from the unresponsive list.
The node can heal only itself. The node responsible only for itself, can't heal other nodes.
This contract simplifies healing algorithm and guaranties that a node becomes available, it marks itself as a responsible
node in the layout. 
 
