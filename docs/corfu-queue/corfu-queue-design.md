The class `CorfuQueue` implements a persisted queue over the abstraction of a CorfuTable. CorfuTable that only uses a HashMap<> to represent the materialized view of a distributed map does not carry a notion of ordering natively. However when implemented over the abstraction of a distributed shared log, the elements added to the map do, in fact, have an ordering imposed by their append or transaction commit operations into the global log. The class `CorfuQueue` attempts to expose this inherent ordering as a persisted Queue with three simple apis:

###  1. `CorfuRecordId enqueue(E)`
Since a map has a key and value, where key is a conflict parameter, enqueue generates a non-conflicting Long as the key and inserts the Entry as a value into a CorfuTable. If enqueue() is wrapped in a Corfu transaction this CorfuRecordId returned will capture the transaction's commit order and together with the entryId define the global cluster-wide order of the entry in the Queue.

### 2. `List<CorfuQueueRecord<Object>> entryList()`
Returns a list of all the entries along with their comparable ids. These ids are packed into CorfuRecordId (convertible into a custom comparable UUID).

### 3. `E remove(CorfuRecordId id)`
Instead of a `dequeue()`, the returned id from `enqueue()` or the `entryList()` api can be used to remove entries in any order from the persisted queue. Note that remove will not change the commit order.

