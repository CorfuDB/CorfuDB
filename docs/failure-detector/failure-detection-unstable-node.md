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
  "failureProbes": [
    "03:08:00",
    "03:08:27",
    "03:08:59",
    "03:09:55",
    "03:11:37"
  ]
}
```

failureProbes - is a parameter that we take from previous layout updates for failures and heals, 
and we ignore state transfer updates.

To calculate a cool-off timeout for the layout we are using exponential backoff: T = interval * Math.pow(rate, iteration) 
 - for last 5 updates: up to 31-min cool-off period
 - rate: 2 times
 - 1 minute interval: 2x of an average failure detection, which is 30 sec, including client updates

| Iteration | Timeout |
|-----------|---------|
| 1         | 1       |
| 2         | 3       |
| 3         | 7       |
| 4         | 15      |
| 5         | 31      |
