package org.corfudb.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/** Checkpoint writer for SMRMaps: take a snapshot of the
 *  object via TXBegin(), then dump the frozen object's
 *  state into CheckpointEntry records into the object's
 *  stream.
 *  TODO: Generalize to all SMR objects.
 */
public class CheckpointWriter<T extends Map> {
    /** Metadata to be stored in the CP's 'dict' map.
     */
    private UUID streamId;
    private String author;
    @Getter
    private UUID checkpointId;
    private LocalDateTime startTime;
    private long startAddress;
    private long endAddress;
    private long numEntries = 0;
    private long numBytes = 0;

    @SuppressWarnings("checkstyle:abbreviation")
    final UUID checkpointStreamID;

    Map<CheckpointEntry.CheckpointDictKey, String> mdkv = new HashMap<>();

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

    /**
     * This option optimizes the checkpoint encoding by grouping multiple
     * map entries into a map instead of using a single smr entry per map
     * entry.
     */
    @Getter
    @Setter
    boolean enablePutAll = false;

    /** Batch size: number of SMREntry in a single CONTINUATION.
     */
    @Getter
    @Setter
    private int batchSize = 50;

    /** BiConsumer to run after every CheckpointEntry is appended to the stream.
     */
    @Getter
    @Setter
    BiConsumer<CheckpointEntry,Long> postAppendFunc = (cp, l) -> { };

    /** Local ref to the object's runtime.
     */
    private CorfuRuntime rt;

    /** Local ref to the stream's view.
     */
    StreamsView sv;

    /** Local ref to the object that we're dumping.
     *  TODO: generalize to all SMR objects.
     */
    private T map;

    @Getter
    @Setter
    ISerializer serializer = Serializers.JSON;

    /** Constructor for Checkpoint Writer for Corfu Maps.
     * @param rt object's runtime
     * @param streamId unique identifier of stream to checkpoint
     * @param author checkpoint initiator
     * @param map local reference of the map to checkpoint
     */
    public CheckpointWriter(CorfuRuntime rt, UUID streamId, String author, T map) {
        this.rt = rt;
        this.streamId = streamId;
        this.author = author;
        this.map = map;
        checkpointId = UUID.randomUUID();
        checkpointStreamID = CorfuRuntime.getCheckpointStreamIdFromId(streamId);
        sv = rt.getStreamsView();
    }

    /** Static method for all steps necessary to append checkpoint
     *  data for a Corfu Map into its own stream.  An optimistic
     *  read-only transaction is used to freeze the contents of
     *  the map while the checkpoint entries are written to
     *  the stream.
     */
    public List<Long> appendCheckpoint() {
        return appendCheckpoint(cpw -> { });
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
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     * @return Global log address of the START record.
     */
    public long startCheckpoint() {
        startTime = LocalDateTime.now();
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        long txBeginGlobalAddress = context.getSnapshotTimestamp();

        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_TIME, startTime.toString());
        // Need the actual object's version
        ICorfuSMR<T> corfuObject = (ICorfuSMR<T>) this.map;
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS,
                Long.toString(corfuObject.getCorfuSMRProxy().getVersion()));
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS,
                Long.toString(txBeginGlobalAddress));

        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> mdkv =
                ImmutableMap.copyOf(this.mdkv);
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointId, streamId, mdkv, null);
        startAddress = sv.append(Collections.singleton(checkpointStreamID), cp, null);

        postAppendFunc.accept(cp, startAddress);
        return startAddress;
    }

    /** Append zero or more CONTINUATION records to this
     *  object's stream.  Each will contain a fraction of
     *  the state of the object that we're checkpointing
     *  (up to batchSize items at a time).
     *
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     *  <p>The Iterators class appears to preserve the laziness
     *  of Stream processing; we don't wish to use more
     *  memory than strictly necessary to generate the
     *  checkpoint.  NOTE: It would be even more useful if
     *  the map had a lazy iterator: the eagerness of
     *  map.keySet().stream() is not ideal, but at least
     *  it should be much smaller than the entire map.</p>
     *
     *  <p>NOTE: The postAppendFunc lambda is executed in the
     *  current thread context, i.e., inside of a Corfu
     *  transaction, and that transaction will be *aborted*
     *  at the end of this function.  Any Corfu data
     *  modifying ops will be undone by the TXAbort().</p>
     *
     * @return Stream of global log addresses of the CONTINUATION records written.
     */
    public List<Long> appendObjectState() {
        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> mdkv =
                ImmutableMap.copyOf(this.mdkv);
        List<Long> continuationAddresses = new ArrayList<>();

        Class underlyingObjectType = ((CorfuCompileProxy<Map>)
                ((ICorfuSMR<T>) map).getCorfuSMRProxy())
                .getObjectType();

        if (enablePutAll) {
            Iterable<List<Object>> partitions = Iterables.partition(map.keySet(), batchSize);

            for (List<Object> partition : partitions) {
                Map tmp = new HashMap();
                for (Object k : partition) {
                    tmp.put(keyMutator.apply(k), valueMutator.apply(map.get(k)));
                }

                SMREntry smrEntry = new SMREntry("putAll", new Object[]{tmp}, serializer
                );
                MultiSMREntry smrEntries = new MultiSMREntry();
                smrEntries.addTo(smrEntry);

                CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                        author, checkpointId, streamId, mdkv, smrEntries);

                long pos = sv.append(Collections.singleton(checkpointStreamID), cp, null);

                postAppendFunc.accept(cp, pos);
                continuationAddresses.add(pos);

                numEntries++;
                // CheckpointEntry::serialize() has a side-effect we use
                // for an accurate count of serialized bytes of SRMEntries.
                numBytes += cp.getSmrEntriesBytes();
            }
        } else {
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
                        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry
                                .CheckpointEntryType.CONTINUATION,
                                author, checkpointId, streamId, mdkv, smrEntries);
                        long pos = sv.append(Collections.singleton(checkpointStreamID), cp, null);

                        postAppendFunc.accept(cp, pos);
                        continuationAddresses.add(pos);

                        numEntries++;
                        // CheckpointEntry::serialize() has a side-effect we use
                        // for an accurate count of serialized bytes of SRMEntries.
                        numBytes += cp.getSmrEntriesBytes();
                    });
        }

        return continuationAddresses;
    }

    /** Append a checkpoint END record to this object's stream.
     *
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     * @return Global log address of the END record.
     */
    public long finishCheckpoint() {
        LocalDateTime endTime = LocalDateTime.now();
        mdkv.put(CheckpointEntry.CheckpointDictKey.END_TIME, endTime.toString());
        numEntries++;
        numBytes++;
        mdkv.put(CheckpointEntry.CheckpointDictKey.ENTRY_COUNT, Long.toString(numEntries));
        mdkv.put(CheckpointEntry.CheckpointDictKey.BYTE_COUNT, Long.toString(numBytes));

        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                author, checkpointId, streamId, mdkv, null);

        endAddress = sv.append(Collections.singleton(checkpointStreamID), cp, null);

        postAppendFunc.accept(cp, endAddress);
        return endAddress;
    }

    /** Start a Global Snapshot Transaction.
     *
     * @param rt corfu runtime
     * @return Global log address of the END record.
     */
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
