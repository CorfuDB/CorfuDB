package org.corfudb.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/** Checkpoint writer for SMRMaps: take a snapshot of the
 *  object via TXBegin(), then dump the frozen object's
 *  state into CheckpointEntry records into the object's
 *  stream.
 *
 * TODO: Generalize to all SMR objects.
 */
public class CheckpointWriter {
    /** Metadata to be stored in the CP's 'dict' map.
     */
    private UUID streamID;
    private String author;
    @Getter
    private UUID checkpointID;
    private LocalDateTime startTime;
    private long startAddress, endAddress;
    private long numEntries = 0, numBytes = 0;
    Map<CheckpointEntry.CheckpointDictKey, String> mdKV = new HashMap<>();

    /** Mutator lambda to change map key.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> keyMutator = (x) -> x;

    /** Mutator lambda to change map value.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> valueMutator = (x) -> x;

    /** Batch size: number of SMREntry in a single CONTINUATION.
     */
    @Getter
    @Setter
    private int batchSize = 50;

    /** BiConsumer to run after every CheckpointEntry is appended
     * to the stream
     */
    @Getter
    @Setter
    BiConsumer<CheckpointEntry,Long> postAppendFunc = (cp, l) -> {};

    /** Local ref to the object's runtime.
     */
    private CorfuRuntime rt;

    /** Local ref to the stream's view.
     */
    StreamsView sv;

    /** Local ref to the object that we're dumping.
     *  TODO: generalize to all SMR objects.
     */
    private SMRMap map;

    @Getter
    @Setter
    ISerializer serializer = Serializers.JSON;

    public CheckpointWriter(CorfuRuntime rt, UUID streamID, String author, SMRMap map) {
        this.rt = rt;
        this.streamID = streamID;
        this.author = author;
        this.map = map;
        checkpointID = UUID.randomUUID();
        sv = rt.getStreamsView();
    }

    /** Static method for all steps necessary to append checkpoint
     *  data for an SMRMap into its own stream.  An optimistic
     *  read-only transaction is used to freeze the contents of
     *  the map while the checkpoint entries are written to
     *  the stream.
     */

    public List<Long> appendCheckpoint() {
        return appendCheckpoint(cpw -> {});
    }

    /**
     * @param setupWriter Lambda to transform the new CheckPointWriter
     *                    object (e.g., change batch size, etc) prior
     *                    to use.
     * @return List of global addresses of all entries for this checkpoint.
     */

    public List<Long> appendCheckpoint(Consumer<CheckpointWriter> setupWriter) {
        List<Long> addrs = new ArrayList<>();
        setupWriter.accept(this);

        startGlobalSnapshotTxn(rt);
        try {
            addrs.add(startCheckpoint());
            addrs.addAll(appendObjectState());
            addrs.add(finishCheckpoint());
            return addrs;
        } finally {
            rt.getObjectsView().TXEnd();
        }
    }

    /** Append a checkpoint START record to this object's stream.
     *
     *  Corfu client transaction management, if desired, is the
     *  caller's responsibility.
     *
     * @return Global log address of the START record.
     */
    public long startCheckpoint() {
        startTime = LocalDateTime.now();
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        long txBeginGlobalAddress = context.getSnapshotTimestamp();

        this.mdKV.put(CheckpointEntry.CheckpointDictKey.START_TIME, startTime.toString());
        this.mdKV.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, Long.toString(txBeginGlobalAddress));

        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> mdKV = ImmutableMap.copyOf(this.mdKV);
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointID, mdKV, null);
        startAddress = sv.append(Collections.singleton(streamID), cp, null);
        postAppendFunc.accept(cp, startAddress);
        return startAddress;
    }

    /** Append zero or more CONTINUATION records to this
     *  object's stream.  Each will contain a fraction of
     *  the state of the object that we're checkpointing
     *  (up to batchSize items at a time).
     *
     *  Corfu client transaction management, if desired, is the
     *  caller's responsibility.
     *
     *  The Iterators class appears to preserve the laziness
     *  of Stream processing; we don't wish to use more
     *  memory than strictly necessary to generate the
     *  checkpoint.  NOTE: It would be even more useful if
     *  the map had a lazy iterator: the eagerness of
     *  map.keySet().stream() is not ideal, but at least
     *  it should be much smaller than the entire map.
     *
     *  NOTE: The postAppendFunc lambda is executed in the
     *  current thread context, i.e., inside of a Corfu
     *  transaction, and that transaction will be *aborted*
     *  at the end of this function.  Any Corfu data
     *  modifying ops will be undone by the TXAbort().
     *
     * @return Stream of global log addresses of the
     * CONTINUATION records written.
     */
    public List<Long> appendObjectState() {
        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> mdKV = ImmutableMap.copyOf(this.mdKV);
        List<Long> continuationAddresses = new ArrayList<>();

        Iterators.partition(map.keySet().stream()
                .map(k -> {
                    return new SMREntry("put",
                            new Object[]{keyMutator.apply(k), valueMutator.apply(map.get(k))},
                            serializer);
                }).iterator(), batchSize)
                .forEachRemaining(entries -> {
                    MultiSMREntry smrEntries = new MultiSMREntry();
                    for (int i = 0; i < ((List) entries).size(); i++) {
                        smrEntries.addTo((SMREntry) ((List) entries).get(i));
                    }
                    CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                            author, checkpointID, mdKV, smrEntries);
                    long pos = sv.append(Collections.singleton(streamID), cp, null);

                    postAppendFunc.accept(cp, pos);
                    continuationAddresses.add(pos);

                    numEntries++;
                    // CheckpointEntry::serialize() has a side-effect we use
                    // for an accurate count of serialized bytes of SRMEntries.
                    numBytes += cp.getSmrEntriesBytes();
                });
        return continuationAddresses;
    }

    /** Append a checkpoint END record to this object's stream.
     *
     *  Corfu client transaction management, if desired, is the
     *  caller's responsibility.
     *
     * @return Global log address of the END record.
     */

    public long finishCheckpoint() {
        LocalDateTime endTime = LocalDateTime.now();
        mdKV.put(CheckpointEntry.CheckpointDictKey.END_TIME, endTime.toString());
        numEntries++;
        numBytes++;
        mdKV.put(CheckpointEntry.CheckpointDictKey.ENTRY_COUNT, Long.toString(numEntries));
        mdKV.put(CheckpointEntry.CheckpointDictKey.BYTE_COUNT, Long.toString(numBytes));

        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                author, checkpointID, mdKV, null);
        endAddress = sv.append(Collections.singleton(streamID), cp, null);

        postAppendFunc.accept(cp, endAddress);
        return endAddress;
    }

    public static long startGlobalSnapshotTxn(CorfuRuntime rt) {
        TokenResponse tokenResponse =
                rt.getSequencerView().nextToken(Collections.EMPTY_SET, 0);
        long globalTail = tokenResponse.getToken().getTokenValue();
        rt.getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(globalTail)
                .begin();
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        return context.getSnapshotTimestamp();
    }
}
