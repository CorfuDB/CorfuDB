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

### Detecting Failures in the Cluster

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

### Disk Failures Detection on a local node
 - data corruption exception:
     - StreamLogFiles#initializeLogMetadata(): corfu fails and can't recover if data on disk is corrupted 
     - StreamLogFiles#initStreamLogDirectory(): during the creation of LogUnitServer, it can go down if disk is read only or log dir is not writable
 - Batch processor failures: can occur in case of an exceptions in BatchProcessor#process() method
 - FileSystemAgent: check that the partition has been mounted

#### Disk failures handing:
 - `FileSystemAgent` will collect information about disk failures (see above)

 - `org.corfudb.infrastructure.management.failuredetector.DecisionMakerAgent` needs to decide:
     - if a node is failed based on the information provided by `FileSystemAgent`
     - if the node needs to be added to the unresponsive list

 - The DecisionMakerAgent has been designed to proactively monitor the health of the system. 
   To achieve this, it will systematically collect detailed statistics from the FileSystemAgent. 
   If the analysis of these statistics reveals any instances of failures, such as a DataCorruptionException, 
   the DecisionMakerAgent will implement a mitigation strategy to exclude the affected node from participating  
   in the current cycle of failure detection in the cluster.

 - The NodeState, which serves as a representation of the status of the node, 
   includes a information of the FileSystem statistics. 
   These statistics, obtained through the systematic collection of data by the FileSystemAgent, 
   will provide all necessary information to the Failure Detector to make a decision about the node failure.

 - Other failure detectors will be able to collect the stats from the node and will decide 
   which node to exclude from the cluster according to the information in FileSystemStats and poll report.

 - Nodes collect file system stats from all the other nodes in the cluster 
   and whichever node gets a decision maker in the cluster on the current iteration can see if another node has problems
   with file system and if the node needs to be excluded from the cluster.

 - `FailuresAgent#detectAndHandleFailure()` is in charge of finding a failed node and 
   figure out if a local node is a decision maker node. 
   If those parameters are met then the agent will trigger the layout update to exclude a node from the cluster

 - Current design of the Failre Detector (with Disk ReadOnly Failure) allows to enrich 
   current FileSystemStats with the new types of failures to have disk probes in it and effectively handle more disk issues

**Changes in `LogUnitServer`:**
 - LogUnitServer (during the creation) will catch DataCorruptionException and IllegalStateException exceptions 
   and send the information to FileSystemAgent

 - LogUnitServer changes `AbstractServer#ServerState` which will indicate whether a node is ready to handle queries or not
   in case of disk issues

 - Failure Detector solves issues with FileSystem with the following steps:
     - by detecting the failure 
     - and then healing the node by executing `HealNodeWorkflow` 
     - which will reset `StreamLogFiles` and triggers data transfer 
     - which will replace corrupted data with the consistent data that the node will collect from the cluster
   
 - Until the node would have issues with ReadOnly file system or the disk partition is not mounted, the node will stay in the unresponsiveList in the layout 

**Changes in `BatchProcessor`:**
 - BatchProcessor will change its state to ERROR in case of exceptions in `BatchProcessor#process()` method
 - FileSystemAgent will collect the status of BatchProcessor
 - DecisionMakerAgent will detect if a node in a failed state the same way how we detect if a disk is in read only mode.
   If so, the node will be added to the unresponsive list.


**Testing**
We will be able to add above scenarios to our test suite, since FileSystemStats contains just information (boolean values) 
about the failure (if DataCorruptionException happened) we can emulate the entire cycle of additional failure detection scenarios.
Like following:
 - BatchProcessorTest#testRestart(): will test proper status of batch processor in case of exceptions in the operations
 - FileSystemAdvisorTest: covers the algorithm of finding failed/healed nodes
 - NodeRankTest: checks proper ordering of the nodes in the list according to their state
 - DecisionMakerAgentTest: checks that the correct decision maker was chosen
 - FailureDetectorServiceTest: tests overall FD mechanism
 - FailureAgentTest: checks the algorithm of finding failed nodes


### Local Node Failure Detection Sequence Diagram

![Local Node Failure Detection Visualization](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/CorfuDB/CorfuDB/master/docs/failure-detector/file-system-failure-detection.puml)


### An example of a possible layout with detected failures and the statistics about the failures

```json
{
  "layoutServers": [
    "192.168.0.1:9000",
    "192.168.0.2:9000",
    "192.168.0.3:9000"
  ],
  "sequencers": [
    "192.168.0.1:9000",
    "192.168.0.2:9000",
    "192.168.0.3:9000"
  ],
  "segments": [
    {
      "replicationMode": "CHAIN_REPLICATION",
      "start": 0,
      "end": -1,
      "stripes": [
        {
          "logServers": [
            "192.168.0.1:9000",
            "192.168.0.2:9001",
            "192.168.0.3:9002"
          ]
        }
      ]
    }
  ],
  "unresponsiveServers": [
    "192.168.0.3:9000",
    "192.168.0.2:9000"
  ],
  "failures": [
    {
      "node": "192.168.0.3:9000",
      "failureType": "ReadOnlyFileSystem",
      "failureDetector": "192.168.0.1:9000",
      "timestamp": "1633972903",
      "clusterState": {
        "nodes": ["192.168.0.1:9000", "192.168.0.2:9000", "192.168.0.3:9000"],
        "connectivity": [
          ["OK", "OK", "FAIL"],
          ["OK", "OK", "FAIL"],
          ["FAIL", "FAIL", "OK"]
        ]
      }
    },
    {
      "node": "192.168.0.2:9000",
      "failureType": "QuotaExceeded",
      "failureDetector": "192.168.0.1:9000",
      "timestamp": "1633972903",
      "clusterState": {
        "nodes": ["192.168.0.1:9000", "192.168.0.2:9000", "192.168.0.3:9000"],
        "connectivity": [
          ["OK", "FAIL", "OK"],
          ["FAIL", "OK", "OK"],
          ["OK", "FAIL", "OK"]
        ]
      }
    }
  ],
  "epoch": 0
}
```

#### "Failures" section in the layout:
- node: failed node
- failureType: type of failure
- failureDetector: the node detected failure (decision maker node)
- timestamp: time when a failure detected by a node
- clusterState: cluster status on a node which detected failure