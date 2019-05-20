The class `CorfuQueue` implements a persisted queue over the abstraction of a CorfuTable. Currently CorfuTable uses a HashMap<> to represent the materialized view of a distributed map. A HashMap does not carry a notion of ordering natively. However when implemented over the abstraction of a distributed shared log, the elements added to the map do, in fact, have an ordering imposed by their append operations into the global log. The class `CorfuQueue` merely exposes this inherent ordering as an immutable logical FIFO Queue interface with three simple apis:

###  1. `EntryId enqueue(E)`
Since a map has a key and value, where key is a conflict parameter, enqueue simply generates a non-conflicting UUID as the key and inserts the Entry as a value into a CorfuTable. The generated UUID is returned for the caller.

### 2. `List<Map.Entry<EntryId, Object>> entryList()`
Returns a list of all the entries along with their ids, sorted by their realized stream addresses (thus logically a FIFO queue). Assuming insertion order is preserved, CorfuTable can materialize its state as a [LinkedHashMap](https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html) instead of a HashMap and return the view as a list.

### 3. `E remove(EntryId id)`
Instead of a `dequeue()`, the returned id from `enqueue()` or the `entryList()` api can be used to remove entries in any order from the persisted queue.

Assuming checkpointing and garbage collection also work in the same insertion order, this proposal is to implement the abstraction of a logical immutable queue using nothing more than a [LinkedHashMap](https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html).
