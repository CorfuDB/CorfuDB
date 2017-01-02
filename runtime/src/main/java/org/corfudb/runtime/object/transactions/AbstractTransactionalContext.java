package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Represents a transactional context. Transactional contexts
 * manage per-thread transaction state.
 *
 * Created by mwei on 4/4/16.
 */
public abstract class AbstractTransactionalContext {

    /** Constant for the address of an uncommitted log entry.
     *
     */
    public static final long UNCOMMITTED_ADDRESS = -1L;

    /** Constant for a transaction which has been folded into
     * another transaction.
     */
    public static final long FOLDED_ADDRESS = -2L;

    /** Constant for a transaction which has been aborted.
     *
     */
    public static final long ABORTED_ADDRESS = -3L;

    /** The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @Getter
    public UUID transactionID;

    /** The builder used to create this transaction.
     *
     */
    @Getter
    public final TransactionBuilder builder;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    /** The global-log position that the transaction snapshots in all reads.
     */
    @Getter(lazy = true)
    private final long snapshotTimestamp = obtainSnapshotTimestamp();


    /** The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    /** The parent context of this transaction, if in a nested transaction.*/
    @Getter
    private final AbstractTransactionalContext parentContext;

    /**
     * A read-set of the txn.
     * We collect the read-set as a map, organized by streams.
     * For each stream, we record:
     *  - a set of conflict-parameters read by this transaction on the stream,
     */
    @Getter
    private final Map<UUID, Set<Integer>> readSet = new HashMap<>();

    /**
     * A write-set is a key component of a transaction.
     * We collect the write-set as a map, organized by streams.
     * For each stream, we record a pair:
     *  - a set of conflict-parameters modified by this transaction on the stream,
     *  - a list of SMR updates by this transcation on the stream.
     *
     * @return a map from streams to write entry representing an update made by this TX
     */
    @Getter
    protected final Map
            <  UUID,                                                // stream ID
                    AbstractMap.SimpleEntry<                        // per-stream pair
                            Set<Integer>,                              // set of conflict-parameters
                            List<WriteSetEntry>                     // list of updates
                    >
            >
            writeSet = new HashMap<>();

    /**
     * A future which gets completed when this transaction commits.
     * It is completed exceptionally when the transaction aborts.
     */
    @Getter
    public CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();

    AbstractTransactionalContext(TransactionBuilder builder) {
        transactionID = UUID.randomUUID();
        this.builder = builder;
        this.startTime = System.currentTimeMillis();
        this.parentContext = TransactionalContext.getCurrentContext();
    }

    /** Access the state of the object.
     *
     * @param proxy             The proxy to access the state for.
     * @param accessFunction    The function to execute, which will be provided with the state
     *                          of the object.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <R>               The return type of the access function.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The return value of the access function.
     */
    abstract public <R,T> R access(ICorfuSMRProxyInternal<T> proxy,
                                   ICorfuSMRAccess<R,T> accessFunction,
                                   Object[] conflictObject);

    /** Get the result of an upcall.
     *
     * @param proxy             The proxy to retrieve the upcall for.
     * @param timestamp         The timestamp to return the upcall for.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The result of the upcall.
     */
    abstract public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                               long timestamp,
                                               Object[] conflictObject);

    /** Log an SMR update to the Corfu log.
     *
     * @param proxy             The proxy which generated the update.
     * @param updateEntry       The entry which we are writing to the log.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The address the update was written at.
     */
    abstract public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry,
                              Object[] conflictObject);

    /** Add a given transaction to this transactional context, merging
     * the read and write sets.
     * @param tc    The transactional context to merge.
     */
    abstract public void addTransaction(AbstractTransactionalContext tc);

    /** Commit the transaction to the log.
     *
     * @throws TransactionAbortedException  If the transaction is aborted.
     */
    public long commitTransaction() throws TransactionAbortedException {
        completionFuture.complete(true);
        return 0L;
    }

    /** Forcefully abort the transaction.
     */
    public void abortTransaction() {
        commitAddress = ABORTED_ADDRESS;
        completionFuture
                .completeExceptionally(new TransactionAbortedException());
    }

    abstract public long obtainSnapshotTimestamp();

    /** Add the proxy and conflict-params information to our read set.
     * @param proxy             The proxy to add
     * @param conflictObjects    The fine-grained conflict information, if
     *                          available.
     */
    public void addToReadSet(ICorfuSMRProxyInternal proxy, Object[] conflictObjects)
    {
        readSet.computeIfAbsent(proxy.getStreamID(), k -> new HashSet<Integer>());
        if (conflictObjects != null) {
            Set<Integer> conflictParamSet = readSet.get(proxy.getStreamID());
            Arrays.asList(conflictObjects).stream()
                .forEach(V -> conflictParamSet.add(Integer.valueOf(V.hashCode())) ) ;
        }
    }

    /**
     * merge another readSet into this one
     * @param otherCSet
     */
    void mergeReadSetInto(Map<UUID, Set<Integer>> otherCSet) {
        otherCSet.forEach((branchID, conflictParamSet) -> {
            this.readSet.computeIfAbsent(branchID, u -> new HashSet<Integer>());
            this.readSet.get(branchID).addAll(conflictParamSet);
        });
    }

    void addToWriteSet(ICorfuSMRProxy proxy, SMREntry updateEntry, Object[] conflictObjects) {

        // create an entry for this streamID
        writeSet.computeIfAbsent(proxy.getStreamID(),
                u -> new AbstractMap.SimpleEntry<>(new HashSet<>(), new ArrayList<>() )
        );

        // add the SMRentry to the list of updates for this stream
        writeSet.get(proxy.getStreamID()).getValue().add(new WriteSetEntry(updateEntry));

        // add all the conflict params to the conflict-params set for this stream
        if (conflictObjects != null) {
            Set<Integer> writeConflictParamSet = writeSet.get(proxy.getStreamID()).getKey();
            Arrays.asList(conflictObjects).stream()
                    .forEach(V -> writeConflictParamSet.add(Integer.valueOf(V.hashCode())));
        }
    }

    /**
     * collect all the conflict-params from the write-set for this transaction into a set.
     * @return A set of longs representing all the conflict params
     */
    Map<UUID, Set<Integer>> collectWriteConflictParams() {
        ImmutableMap.Builder<UUID, Set<Integer>> builder = new ImmutableMap.Builder<>();

        writeSet.entrySet()         // mappings from streamIDs to
                                    // pairs (set of conflict-params, list of WriteSetEntry)
                .forEach(e -> {
                    builder.put(e.getKey(),     // UUID
                                e.getValue()   // a pair
                                   .getKey() );// left component: a conflict-params set
                    });
        return builder.build();
    }

    void mergeWriteSetInto(Map<UUID, AbstractMap.SimpleEntry<Set<Integer>, List<WriteSetEntry>>> otherWSet) {
        otherWSet.entrySet().forEach(e-> {
            // create an entry for this streamID
            writeSet.computeIfAbsent(e.getKey(),                    // the streamID
                    u -> new AbstractMap.SimpleEntry<>(new HashSet<>(), new ArrayList<>() ));

            // copy all the conflict-params set for this streamID
            writeSet.get(e.getKey())                 // the entry pair for this streamID
                    .getKey()                        // the left componentof the pair is a conflict-param set
                    .addAll(e.getValue()             // the pair from the otherWSet
                            .getKey());              // the left component of the pair

            // copy all the WriteSetEntry list for this streamID
            writeSet.get(e.getKey())                 // the entry pair for this streamID
                    .getValue()                      // the right componentof the pair is a WriteSetEntry list
                    .addAll(e.getValue()             // the pair from the otherWSet
                            .getValue());            // the right component of the pair


        });
    }

    /**
     * convert our write set into a new MultiObjectSMREntry.
     * @return
     */
    MultiObjectSMREntry collectWriteSetEntries() {
        ImmutableMap.Builder<UUID, MultiSMREntry> builder =
                ImmutableMap.builder();

        writeSet.entrySet()                 // mappings from streamIDs to
                // pairs (set of conflict-params, list of WriteSetEntry)

                .forEach(x -> builder.put(x.getKey(),   // a streamID
                        new MultiSMREntry(x.getValue()  // a pair
                                .getValue()             // right component: a list of WriteSetEntry
                                .stream()
                                .map(WriteSetEntry::getEntry)
                                .collect(Collectors.toList())))
                );
        Map<UUID, MultiSMREntry> entryMap = builder.build();
        MultiObjectSMREntry entry = new MultiObjectSMREntry(entryMap);
        return entry;
    }

    /** Helper function to get a write set for a particular stream.
     *
     * @param id    The stream to get a write set for.
     * @return      The write set for that stream, as an ordered list.
     */
    List<WriteSetEntry> getWriteSetEntryList(UUID id) {

        return writeSet
                .getOrDefault(id, new AbstractMap.SimpleEntry<>(Collections.emptySet(), Collections.emptyList()))
                .getValue();
    }


}
