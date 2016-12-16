package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A standard Corfu optimistic transaction context.
 *
 * Optimistic transactions in Corfu provide the following guarantees:
 *
 * (1) Reads in a transaction are guaranteed to observe either
 *  (a) A write in the same transaction, if a write happens before
 *      the read.
 *  (b) The state of the system ("snapshot") as of the time of the
 *      first read which occurs in the transaction ("first read
 *      timestamp").
 *
 * (2) Writes in a transaction are guaranteed to commit atomically,
 *     and commit if and only if none of the objects which were
 *     read (the "read set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 *
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    /**
     * The timestamp of the first read in the system.
     *
     * @return The timestamp of the first read object, which may be null.
     */
    @Getter(lazy = true)
    private final long firstReadTimestamp = fetchFirstTimestamp();

    OptimisticTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }

    /** The write set for this transaction.*/
    @Getter
    private Map<UUID, List<UpcallWrapper>> writeSet = new ConcurrentHashMap<>();

    /** The read set for this transaction. */
    @Getter
    private Set<UUID> readSet = new HashSet<>();

    /** The proxies which were modified by this transaction. */
    @Getter
    private Set<ICorfuSMRProxyInternal> modifiedProxies = new HashSet<>();

    /**
     * Sync the state of the proxy to the latest updates in the write
     * set for a stream.
     * @param proxy             The proxy which we are playing forward.
     * @param <T>               The type of the proxy's underlying object.
     */
    private <T> void syncUnsafe(ICorfuSMRProxyInternal<T> proxy) {

        // Commonly called fields, for readability.
        final VersionLockedObject<T> object = proxy.getUnderlyingObject();
        final UUID streamID = proxy.getStreamID();

        try {
            // If we don't own this object, roll it back
            if (object.getModifyingContextUnsafe() != this) {
                object.optimisticRollbackUnsafe();
            }

            // If the version of this object is ahead of what we expected,
            // we need to rollback...
            if (object.getVersionUnsafe() > getFirstReadTimestamp()) {
                // We don't yet support version rollback, but we would
                // perform that here when we do.
                throw new NoRollbackException();
            }
        } catch (NoRollbackException nre) {
            // Couldn't roll back the object, so we'll have
            // to start from scratch.
            // TODO: create a copy instead
            proxy.resetObjectUnsafe(object);
        }

        // next, if the version is older than what we need
        // sync.
        if (object.getVersionUnsafe() < getFirstReadTimestamp()) {
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    getFirstReadTimestamp());
        }

        // Take ownership of the object.
        object.setTXContextUnsafe(this);

        // Collect all the optimistic updates for this object, in order
        // which they need to be applied.
        List<UpcallWrapper> allUpdates = new LinkedList<>();

        Iterator<AbstractTransactionalContext> contextIterator =
            TransactionalContext.getTransactionStack().descendingIterator();

        contextIterator.forEachRemaining(x -> {
            allUpdates.addAll(x.getWriteSet()
                    .getOrDefault(streamID, Collections.emptyList()));
        });

        // Record that we have modified this proxy.
        if (allUpdates.size() > 0) {
            modifiedProxies.add(proxy);
        }

        // Apply all the updates from the optimistic version to the end
        // of the list of all updates for this thread.
        IntStream.range(object.getOptimisticVersionUnsafe(), allUpdates.size())
                .mapToObj(allUpdates::get)
                .forEachOrdered(wrapper -> {
                    SMREntry entry  = wrapper.getEntry();
                    Object res = proxy.getUnderlyingObject()
                            .applyUpdateUnsafe(entry, true);
                    wrapper.setUpcallResult(res);
                    wrapper.setHaveUpcallResult(true);
                });
    }

    /** Access the underlying state of the object.
     *
     * @param proxy             The proxy making the state request.
     * @param accessFunction    The access function to execute.
     * @param <R>               The return type of the access function.
     * @param <T>               The type of the proxy.
     * @return                  The result of the access.
     */
    @Override
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy, ICorfuSMRAccess<R, T> accessFunction) {
        // First, we add this access to the read set.
        readSet.add(proxy.getStreamID());

        // Next, we check if the write set has any
        // outstanding modifications.
        try {
            return proxy.getUnderlyingObject().optimisticallyReadAndRetry((v, o) -> {
                // to ensure snapshot isolation, we should only read from
                // the first read timestamp.
                if (v == getFirstReadTimestamp() &&
                        objectIsOptimisticallyUpToDateUnsafe(proxy))
                {
                    return accessFunction.access(o);
                }
                throw new ConcurrentModificationException();
            });
        } catch (ConcurrentModificationException cme) {
            // It turned out version was wrong, so we're going to have to do
            // some work.
        }

        // Now we're going to do some work to modify the object, so take the write
        // lock.
        return proxy.getUnderlyingObject().write((v, o) -> {
            syncUnsafe(proxy);
            // We might have ended up with a _different_ object
            return accessFunction.access(proxy.getUnderlyingObject()
                    .getObjectUnsafe());
        });
    }

    /** Obtain the result for an upcall. Since we are executing on a single thread,
     * The result of the upcall is just the last one stored.
     * @param proxy         The proxy making the request.
     * @param timestamp     The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The result of the upcall.
     */
    @Override
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy, long timestamp) {
        // Getting an upcall result adds the object to the read set.
        readSet.add(proxy.getStreamID());
        // if we have a result, return it.
        UpcallWrapper wrapper = getWriteSet(proxy.getStreamID()).get((int)timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()){
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().write((v,o) -> {
            syncUnsafe(proxy);
            UpcallWrapper wrapper2 = getWriteSet(proxy.getStreamID()).get((int)timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()){
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but" +
            " we don't have it even after an optimistic sync (asked for " + timestamp +
            " we have 0-" + (writeSet.get(proxy.getStreamID()).size() - 1) + ")");
        });
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The "address" that the update was written to.
     */
    @Override
    public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy, SMREntry updateEntry) {
        writeSet.putIfAbsent(proxy.getStreamID(), new LinkedList<>());
        writeSet.get(proxy.getStreamID()).add(new UpcallWrapper(updateEntry));
        return writeSet.get(proxy.getStreamID()).size() - 1;
    }

    /**
     * Commit a transaction into this transaction by merging the read/write sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
        // merge the read sets and write sets
        readSet.addAll(tc.getReadSet());
        tc.getWriteSet().entrySet().forEach(e-> {
            writeSet.putIfAbsent(e.getKey(), new LinkedList<>());
            writeSet.get(e.getKey()).addAll(e.getValue());
        });
        // "commit" the optimistic writes (for each proxy we touched)
        // by updating the modifying context (as long as the context
        // is still the same).
        updateAllProxies(x ->
                x.getUnderlyingObject()
                    .setTXContextUnsafe(this));
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * write it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @Override
    @SuppressWarnings("unchecked")
    public long commitTransaction() throws TransactionAbortedException {

        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            return commitAddress;
        }

        // Otherwise, commit by generating the set of affected streams
        // and having the sequencer conditionally issue a token.
        Set<UUID> affectedStreams = writeSet.keySet();

        // For now, we have to convert our write set into a map
        // that we can construct a new MultiObjectSMREntry from.
        ImmutableMap.Builder<UUID, MultiSMREntry> builder =
                ImmutableMap.builder();
        writeSet.entrySet()
                .forEach(x -> builder.put(x.getKey(),
                                          new MultiSMREntry(x.getValue().stream()
                                                            .map(UpcallWrapper::getEntry)
                                                            .collect(Collectors.toList()))));
        Map<UUID, MultiSMREntry> entryMap = builder.build();
        MultiObjectSMREntry entry = new MultiObjectSMREntry(entryMap);

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = this.builder.runtime.getStreamsView()
                .acquireAndWrite(affectedStreams, entry, t->true, t->true,
                        getFirstReadTimestamp(), readSet);
        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            abortTransaction();
            throw new TransactionAbortedException();
        }

        super.commitTransaction();
        commitAddress = address;

        // Update all proxies, committing the new address.
        updateAllProxies(x ->
                x.getUnderlyingObject()
                        .optimisticCommitUnsafe(commitAddress));

        return address;
    }

    @SuppressWarnings("unchecked")
    protected void updateAllProxies(Consumer<ICorfuSMRProxyInternal> function) {
        getModifiedProxies().forEach(x -> {
            // If we are on the same thread, this will hold true.
            if (x.getUnderlyingObject().getModifyingContextUnsafe()
                    == this) {
                x.getUnderlyingObject().writeReturnVoid((v,o) -> {
                    // Make sure we're still the modifying thread
                    // even after getting the lock.
                    if (x.getUnderlyingObject().getModifyingContextUnsafe()
                            == this) {
                        function.accept(x);
                    }
                });
            }
        });
    }

    /** Helper function to get a write set for a particular stream.
     *
     * @param id    The stream to get a write set for.
     * @return      The write set for that stream, as an ordered list.
     */
    private List<UpcallWrapper> getWriteSet(UUID id) {
        return writeSet.getOrDefault(id, new LinkedList<>());
    }



    /** Determine whether a proxy's object is optimistically "up to date".
     *
     * An object is optimistically up to date if
     *
     * (1) There are no writes in any transaction's write set and there
     * are no optimistic writes on the object.
     *
     * (2) There are optimistic writes on the object and the number of
     * optimistic writes is equal to the number of writes in the transactions
     * write set, and the optimistic writes were OUR modifications.
     *
     * @param proxy     The proxy containing the object to check.
     * @param <T>       The underlying type of the object for the proxy.
     * @return          True, if the object is optimistically up to date.
     *                  False otherwise.
     */
    private <T> boolean objectIsOptimisticallyUpToDateUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        long numOptimisticModifications = TransactionalContext.getTransactionStack()
                .stream().flatMap(x -> x.getWriteSet().getOrDefault(proxy.getStreamID(),
                        Collections.emptyList()).stream()).count();

        return (numOptimisticModifications == 0 &&
                !proxy.getUnderlyingObject().isOptimisticallyModifiedUnsafe()) ||
                (proxy.getUnderlyingObject().getModifyingContextUnsafe() == this &&
                        proxy.getUnderlyingObject().getOptimisticVersionUnsafe() == numOptimisticModifications);
    }



    /** Get the root context (the first context of a nested txn)
     * which must be an optimistic transactional context.
     * @return  The root context.
     */
    private OptimisticTransactionalContext getRootContext() {
        AbstractTransactionalContext atc = TransactionalContext.getRootContext();
        if (!(atc instanceof OptimisticTransactionalContext)) {
            throw new RuntimeException("Attempted to nest two different transactional context types");
        }
        return (OptimisticTransactionalContext)atc;
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    private synchronized long fetchFirstTimestamp() {
        if (getRootContext() != this) {
            // If we're in a nested transaction, the first read timestamp
            // needs to come from the root.
            return getRootContext().getFirstReadTimestamp();
        } else {
            // Otherwise, fetch a read token from the sequencer the linearize
            // ourselves against.
            long token = builder.runtime
                    .getSequencerView().nextToken(Collections.emptySet(), 0).getToken();
            log.trace("Set first read timestamp for tx {} to {}", transactionID, token);
            return token;
        }
    }

}
