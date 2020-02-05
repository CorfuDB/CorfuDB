The class `CorfuQueue` implements a persisted queue over the abstraction of a CorfuTable. CorfuTable that only uses a HashMap<> to represent the materialized view of a distributed map does not carry a notion of ordering natively. However when implemented over the abstraction of a distributed shared log, the elements added to the map do, in fact, have an ordering imposed by their append or transaction commit operations into the global log. The class `CorfuQueue` attempts to expose this inherent ordering as a persisted Queue with three simple apis:

###  1. `CorfuRecordId enqueue(E)`
Since a map has a key and value, where key is a conflict parameter, enqueue generates a non-conflicting Long as the key and inserts the Entry as a value into a CorfuTable. The generated Long is packed into the LSB of a UUID and returned to the caller as a CorfuRecordId. Note that this Id does not carry ordering since the operation could be part of a transaction that has not committed yet.

### 2. `List<CorfuQueueRecord<Object>> entryList()`
Returns a list of all the entries along with their ids. These ids are packed into CorfuRecordId (UUIDs) with the MSB bits composed out of their (stream snapshot address, index in queue) tuples and their LSB bits with the enqueued Long. Thus these Ids have a global comparable ordering maintained by their underlying CorfuTable's [LinkedHashMap](https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html). Note that the CorfuRecordId returned from the enqueue() operation should not be directly compared with that returned from the entryList() api because one does not have ordering while the other does.

### 3. `E remove(CorfuRecordId id)`
Instead of a `dequeue()`, the returned id from `enqueue()` or the `entryList()` api can be used to remove entries in any order from the persisted queue.

Assuming checkpointing and garbage collection also work in the same insertion order, the abstraction of a logical queue can be done using nothing more than a CorfuTable over a [LinkedHashMap](https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html) instead of a simple HashMap<>.
