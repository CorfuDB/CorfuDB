package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.annotation.Immutable;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    @Getter
    private boolean firstReadTimestampSet = false;

    /**
     * The timestamp of the first read in the system.
     *
     * @return The timestamp of the first read object, which may be null.
     */
    @Getter(lazy = true)
    private final long firstReadTimestamp = fetchFirstTimestamp();

    public OptimisticTransactionalContext(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    public synchronized long fetchFirstTimestamp() {
        firstReadTimestampSet = true;
        long token = runtime.getSequencerView().nextToken(Collections.emptySet(), 0).getToken();
        log.trace("Set first read timestamp for tx {} to {}", transactionID, token);
        return token;
    }

    /**
     * Compute and write a TXEntry for this transaction to insert into the log.
     *
     * @return A TXEntry which represents this transactional context.
     */
    public TXEntry getEntry() {
        Map<UUID, TXEntry.TXObjectEntry> entryMap = new HashMap<>();

        // newer TX stuff
        writeSet.entrySet().forEach(x -> {
                        List<SMREntry> entries = x.getValue().stream()
                                                    .map(UpcallWrapper::getEntry)
                                                    .collect(Collectors.toList());

                            entryMap.put(x.getKey(),
                            new TXEntry.TXObjectEntry(entries, false));
                });

        readSet.forEach(x -> {
            if (entryMap.containsKey(x)) {
                entryMap.get(x).setRead(true);
            }
            else {
                entryMap.put(x, new TXEntry.TXObjectEntry(Collections.emptyList(),
                        true));
            }
        });


        return new TXEntry(entryMap, isFirstReadTimestampSet() ? getFirstReadTimestamp() : -1L);
    }

    /** A wrapper which combines SMREntries with
     * their upcall result.
     */
    @Data
    @RequiredArgsConstructor
    private static class UpcallWrapper {
        final SMREntry entry;
        Object upcallResult;
        boolean haveUpcallResult;
    }

    /** The write set for this transaction.*/
    private Map<UUID, List<UpcallWrapper>> writeSet = new ConcurrentHashMap<>();

    /** The read set for this transaction. */
    private Set<UUID> readSet = new HashSet<>();

    /** The write pointers for this transaction.
     * For each proxy, this map keeps track of the furthest
     * position each proxy has synced to.
     */
    private Map<ICorfuSMRProxyInternal, Integer> writeSetPointer = new ConcurrentHashMap<>();

    /** Helper function to get the current write set pointer for a proxy.
     *
     * @param proxy     The proxy to get the write set pointer for
     * @return          The current write set pointer for the proxy.
     */
    private int getWriteSetPointer(ICorfuSMRProxyInternal proxy) {
        return writeSetPointer.getOrDefault(proxy, 0);
    }

    /** Helper function to increment the write set pointer for a proxy.
     *
     * @param proxy     The proxy to increment the write set pointer to.
     */
    private void incrementWriteSetPointer(ICorfuSMRProxyInternal proxy) {
        writeSetPointer.compute(proxy, (k,v) -> {
            if (v == null) return 0;
            return v+1;
        });
    }

    /** Helper function to clear the write set pointer.
     *
     * @param proxy     The proxy to clear the write set pointer for.
     */
    private void clearWriteSetPointer(ICorfuSMRProxyInternal proxy) {
        // We have to be careful when clearing, since the
        // writeSetPointer is used to track objects we've
        // modified.
        writeSetPointer.computeIfPresent(proxy, (k,v) -> 0);
    }

    /** Helper function to get a write set for a particular stream.
     *
     * @param id    The stream to get a write set for.
     * @return      The write set for that stream, as an ordered list.
     */
    private List<UpcallWrapper> getWriteSet(UUID id) {
        return writeSet.getOrDefault(id, new LinkedList<>());
    }

    /**
     * Roll back the optimistic updates we have made to a proxy,
     * restoring the state of the underlying object.
     * @param proxy             The proxy which we are rolling back.
     * @param <T>               The type of the proxy's underlying object.
     */
    @Override
    public <T> void rollbackUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        // starting at the write pointer, roll back any
        // updates to the object we've applied
        try {
            // can we rollback all the updates? if not, abort and sync
            if (getWriteSet(proxy.getStreamID()).stream()
                    .anyMatch(x -> !x.getEntry().isUndoable())) {
                throw new RuntimeException("Some updates were not undoable");
            }
            IntStream.range(0, getWriteSetPointer(proxy))
                    .map(x -> getWriteSetPointer(proxy) - x - 1) // reverse the stream
                    .mapToObj(x -> getWriteSet(proxy.getStreamID()).get(x))
                    .forEachOrdered(x -> {
                        // Undo the operation, if this fails, we'll throw an exception
                        // and sync.
                        proxy.getUndoTargetMap().get(x.getEntry().getSMRMethod())
                            .doUndo(proxy.getUnderlyingObject().getObjectUnsafe(),
                                    x.getEntry().getUndoRecord(),
                                    x.getEntry().getSMRArguments());
                    });
            // Lift our transactional context
            clearWriteSetPointer(proxy);
            proxy.getUnderlyingObject().setTXContextUnsafe(null);
        } catch (Exception e) {
            // rolling back failed, so we'll resort to getting fresh state
            proxy.resetObjectUnsafe(proxy.getUnderlyingObject());
            proxy.getUnderlyingObject().setTXContextUnsafe(null);
            clearWriteSetPointer(proxy);
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    proxy.getVersion());
        }
    }

    /**
     * Sync the state of the proxy to the latest updates in the write
     * set for a stream.
     * @param proxy             The proxy which we are playing forward.
     * @param <T>               The type of the proxy's underlying object.
     */
    @Override
    public <T> void syncUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        // first, if some other thread owns this object
        // we'll try waiting, but if they take too long
        // we should steal it from them
        if (    proxy.getUnderlyingObject()
                .getTXContextUnsafe() != null &&
                !proxy.getUnderlyingObject()
                .isTXOwnedByThisThread())
        {
            Deque<AbstractTransactionalContext> otherTXStack =
                    proxy.getUnderlyingObject()
                    .getTXContextUnsafe();

            // TODO: this is not going to be effective until
            // we release the lock. The other tx will not be
            // able to complete until we do.
            // There has to be at least one element present here
            // otherwise we wouldn't be owned by another thread.
            //try {
            //    otherTXStack.peek().completionFuture
            //            .get(100, TimeUnit.MILLISECONDS);
            //} catch (InterruptedException | ExecutionException |
            //        TimeoutException e) {
            //    // We don't care if the tx is aborted or canceled.
            //}

            // need to rollback but that means we need to have a list
            // of tx from the other thread!
            //proxy.getUnderlyingObject().getTXContextUnsafe()
            //        .forEach(x -> {
            //            x.rollbackUnsafe(proxy);
            //        });
        }
        // next, if the version is incorrect, we need to
        // sync.
        if (proxy.getVersion() != getFirstReadTimestamp()) {
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    getFirstReadTimestamp());
        }
        // finally, if we have buffered updates in the write set,
        // we need to apply them.

        if ((getWriteSet(proxy.getStreamID()).size()
                != getWriteSetPointer(proxy))) {
            proxy.getUnderlyingObject()
                    .setTXContextUnsafe(TransactionalContext.getTransactionStack());
            IntStream.range(getWriteSetPointer(proxy),
                    getWriteSet(proxy.getStreamID()).size())
                    .mapToObj(x -> getWriteSet(proxy.getStreamID()).get(x))
                    .forEach(wrapper -> {
                        SMREntry entry  = wrapper.getEntry();
                        // Find the upcall...
                        ICorfuSMRUpcallTarget<T> target =
                                proxy.getUpcallTargetMap().get(entry.getSMRMethod());
                        incrementWriteSetPointer(proxy);
                        if (target == null) {
                            throw new
                                    RuntimeException("Unknown upcall " + entry.getSMRMethod());
                        }
                        // Can we generate an undo record?
                        IUndoRecordFunction<T> undoRecordTarget =
                                proxy.getUndoRecordTargetMap().get(entry.getSMRMethod());
                        if (undoRecordTarget != null) {
                            entry.setUndoRecord(undoRecordTarget
                                    .getUndoRecord(proxy.getUnderlyingObject()
                                                    .getObjectUnsafe(),
                                            entry.getSMRArguments()));
                            entry.setUndoable(true);
                        }

                        try {
                             wrapper.setUpcallResult(target.upcall(proxy.getUnderlyingObject()
                                    .getObjectUnsafe(), entry.getSMRArguments()));
                             wrapper.setHaveUpcallResult(true);
                        }
                        catch (Exception e) {
                            log.error("Error: Couldn't execute upcall due to {}", e);
                            throw new RuntimeException(e);
                        }
                    });
        }
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
        // This will determine whether or not we need
        // a write lock.
        if (getWriteSet(proxy.getStreamID()).size() == getWriteSetPointer(proxy)) {
            // If the version is correct, now we can try
            // to service the read by taking a read lock.
            try {
                return proxy.getUnderlyingObject().optimisticallyReadAndRetry((v, o) -> {
                    // to ensure snapshot isolation, we should only read from
                    // the first read timestamp.
                    if (v == getFirstReadTimestamp()) {
                        return accessFunction.access(o);
                    }
                    throw new ConcurrentModificationException();
                });
            } catch (ConcurrentModificationException cme) {
                // It turned out version was wrong, so we're going to have to do
                // some work.
            }
        }

        // Now we're going to do some work to modify the object, so take the write
        // lock.
        return proxy.getUnderlyingObject().write((v, o) -> {
            syncUnsafe(proxy);
            return accessFunction.access(o);
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
        // if we have a result, return it.
        UpcallWrapper wrapper = writeSet.get(proxy.getStreamID()).get((int) timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()){
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().write((v,o) -> {
            syncUnsafe(proxy);
            UpcallWrapper wrapper2 = writeSet.get(proxy.getStreamID()).get((int) timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()){
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but" +
            "we don't have it even after an optimistic sync");
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

    /** Abort this transaction context, restoring the state
     * of any object we've touched.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void abortTransaction() {
        // Every object with a write set will have
        // an active write set pointer.
        writeSetPointer.keySet().forEach(proxy ->
            proxy.getUnderlyingObject().writeReturnVoid((ver, obj) -> {
                rollbackUnsafe(proxy);
            }));
        super.abortTransaction();
    }

    /** Determine whether or not we can abort all the writes
     * in this transactions write set.
     * @param streamID  The stream to check
     * @return  True, if all writes can be aborted.
     */
    @Override
    public boolean canUndoTransaction(UUID streamID) {
        // This is safe, because the thread checking
        // if a transaction will be undone will hold
        // the write lock - for that object.
        return writeSet.get(streamID).stream()
                .allMatch(x -> x.getEntry().isUndoable());
    }

    /**
     * Commit a transaction into this transaction by merging the read/write sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
            // flatter newer tx maps - for now we only support other optimistic txns
            if (!(tc instanceof OptimisticTransactionalContext)) {
                throw new RuntimeException("only optimistic txns are supported");
            }
            OptimisticTransactionalContext opt = (OptimisticTransactionalContext)
                    tc;
            // make sure the txn is syncd for all proxies
            readSet.addAll(opt.readSet);
            opt.writeSet.entrySet().stream().forEach(e-> {
                writeSet.putIfAbsent(e.getKey(), new LinkedList<>());
                writeSet.get(e.getKey()).addAll(e.getValue());
                // also update all the pointers
                Set<ICorfuSMRProxyInternal> proxies = writeSetPointer
                        .keySet().stream()
                        .filter(x -> x.getStreamID().equals(e.getKey()))
                        .collect(Collectors.toSet());;
                proxies.forEach(x -> {
                    x.getUnderlyingObject().writeReturnVoid((v,o) -> {
                        opt.syncUnsafe(x);
                    });
                    writeSetPointer.put(x, e.getValue().size());
                });
            });
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * write it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @Override
    public long commitTransaction() throws TransactionAbortedException {

        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {

            return AbstractTransactionalContext.FOLDED_ADDRESS;
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
        long address = runtime.getStreamsView()
                .acquireAndWrite(affectedStreams, entry, t->true, t->true, getFirstReadTimestamp());
        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            abortTransaction();
            throw new TransactionAbortedException();
        }

        super.commitTransaction();

        return address;
    }
}
