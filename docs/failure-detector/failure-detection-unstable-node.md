### Failure Detector to detect an unstable node

In certain operational scenarios, we encounter situations where a node within a cluster repeatedly connects to and
disconnects from the cluster.
This behavior can point to potential issues when too many updates make a cluster unstable.

One proposed way to enhance the Failure Detector mechanism in Corfu is by introducing rules
that control how often individual nodes can update the layout.
This enhancement involves a dynamic scoring system that keeps track of each node's activity regarding layout updates.
When a node starts making frequent layout changes, its score gradually increases.
The higher the score, the less frequently the node is allowed to make further layout updates.

The underlying idea behind this approach is to limit the undesirable outcome of constant changes to the database layout.
By assigning penalties to nodes with high scores, the system effectively limits their ability to carry out frequent
layout modifications.
As a result, this contributes to improving the overall stability of the Corfu db.

In practice, this scoring-based constraint mechanism represents an approach to addressing a particular scenario of an
erratic node behavior,
aligning the cluster more closely with the desired operational reliability and stability.

#### The Solution

The solution is limit the number of layout updates, preventing uncontrollable layout updates by nodes.
What we get from the improvement:

- limit the number of layout updates
- let a node heal itself automatically after a timeout if the node was unstable (a backoff strategy - exponential)
- see when network was unstable by looking into layout files
- dynamically balance layout updates rather than have hardcoded limits for the updates

The design is to add a section into the layout like so:

LAYOUTS_CURRENT.ds:

```json
{
  "probes": [
    1, "03:08:00",
    2, "03:08:27",
    3, "03:08:59",
    3, "03:09:55",
  ],
  "status": "RED"
}
```

failureProbes - is a parameter that we take from previous layout updates ONLY for FAILURES and HEALING, 
and we ignore any other layout and epoch updates, including state transfer updates.

To calculate a cool-off timeout for the layout we are using exponential backoff: T = interval * Math.pow(rate, iteration) 
 - for last 3 updates: up to 7-min cool-off period
 - rate: 2 times
 - 1 minute interval: 2x of an average failure detection, which is 30 sec, including client updates

| Iteration | Timeout |
|-----------|---------|
| 1         | 1       |
| 2         | 3       |
| 3         | 7       |

Those 3 states will be represented as: GREEN, YELLOW, RED.

In case of multiple failures we will increase the timeout of healing node and prevent cluster from being constantly get updated.
The price for that possible unavailability of the cluster, since essentially we slow down failure detection mechanism,
but in case of if the network is unstable there is nothing much to do, no reason for frequent updates of the layout too.

The algorithm is applied only during "HEAL" operation, we check the timeouts (see the table) and if the layout was 
updated more times than it allowed then failure detector is not allowed to continue and remove the node from the unresponsive list
and needs to wait for: X = "Cool-off Timeout" - time passed by last 3 updates.

#### Scenario 1:
Notes: 
 - update - means healing operation
```
00:00:30 -> 
{
    cluster: unresponsive[NodeA],
    
    // Latest updates of the layout. 
      - timeout: 1,3,7 minutes, 
      - count: how many time the updates happened, 
      - limit: allowed number of updates 
    probes: [
        { timeout: 1, time: 00:00:30}
        { timeout: 3, time: None}
        { timeout: 7, time: None}
    ]
    
    status: OK,
    note: starting point, empty counters
}

00:00:47 -> 
{
    cluster: unresponsive[],
    probes: [
        { timeout: 1, count: 1, limit: 1, time: 00:00:47} // 00:00:47-00:00:30=17 sec -> less than a minute, increment all counters 
        { timeout: 3, count: 1, limit: 2, time: 00:00:30}
        { timeout: 7, count: 1, limit: 3, time: 00:00:30}
    ]
    status: OK,
    note: first healing
}

00:01:03 ->
{
    cluster: unresponsive[NodeA],
    probes: [
        { timeout: 1, count: 1, limit: 1, time: 00:00:47 } // 00:00:47-00:00:30=17 sec -> less than a minute, increment all counters 
        { timeout: 3, count: 1, limit: 2, time: 00:00:30 }
        { timeout: 7, count: 1, limit: 3, time: 00:00:30 }
    ]
    status: OK,
    note: starting point, empty counters
}
{
    cluster: ,
    probes: [
        { timeout: 1, count: 1, limit: 1}
        { timeout: 3, count: 1, limit: 2}
        { timeout: 7, count: 1, limit: 3}
    ]
    status: OK,
    note: failure detection, NodeA again. We let it go, we handle only healing
}

00:01:15 ->
{
    iteration: 2, 
    cluster: unresponsive[], 
    status: REJECTED
    note: time diff between previous heal and the current one is: 00:01:15-00:00:47=28 seconds.
          The allowed time out is 1 minute.
}

00:01:50 ->
{
    iteration: 2, 
    cluster: unresponsive[], 
    status: OK
    note: the timeout is more than a minute, we are ok to heal. Next iteration is allowed after 3 min timeout
}

00:01:50 ->
{
    iteration: 2, 
    cluster: unresponsive[], 
    status: OK
    note: the timeout is more than a minute, we are ok to heal
} 

```