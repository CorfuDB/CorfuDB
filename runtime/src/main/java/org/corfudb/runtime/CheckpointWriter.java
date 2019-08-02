package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.CacheOption;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;


/** Checkpoint writer for SMRMaps: take a snapshot of the
 *  object via TXBegin(), then dump the frozen object's
 *  state into CheckpointEntry records into the object's
 *  stream.
 *  TODO: Generalize to all SMR objects.
 */
@Slf4j
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

    // Registry and Timer used for measuring append checkpoint
    private static MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
    private static final String CHECKPOINT_TIMER_NAME = CorfuComponent.GARBAGE_COLLECTION +
            "append-checkpoint";
    private Timer appendCheckpointTimer = metricRegistry.timer(CHECKPOINT_TIMER_NAME);

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

    /**
     * Append a checkpoint, i.e., coalesce the state of the log. The snapshot
     * of this checkpoint will be internally computed as to the current tail of the log.
     *
     * @return Token at which the snapshot for this checkpoint was taken.
     */
    public Token appendCheckpoint() {
        // Queries the sequencer for the stream tail. We then read the stream tail to
        // persist the entry on the log unit in turn preventing a new sequencer from
        // regressing tokens.
        Token snapshot = rt.getSequencerView().query(streamId).getToken();
        return appendCheckpoint(snapshot);
    }

    /**
     * Write a checkpoint which reflects the state at snapshot.
     *
     * This API should not be directly invoked.
     *
     *  @param snapshotTimestamp snapshot at which the checkpoint is taken.
     */
    @VisibleForTesting
    public Token appendCheckpoint(Token snapshotTimestamp) {
        long start = System.currentTimeMillis();

        // If snapshotTimestamp (stream tail) is a negative value
        // we need to enforce a hole to ensure the start log address
        // is not negative (as MultiCheckpointWriter will select the min of all checkpoints)
        // Having a negative value will prevent trimming from happening.
        if (!Address.isAddress(snapshotTimestamp.getSequence())) {
            snapshotTimestamp = forceHole();
        }

        try {
            rt.getAddressSpaceView().read(snapshotTimestamp.getSequence());
        } catch (TrimmedException te) {
            // Stream Tail was trimmed, we can just ignore this trim. This means
            // no other update has been done to this stream since trim.
            log.info("appendCheckpointOnSnapshot: stream tail at {} has been trimmed. Checkpoint on same state" +
                    "as previous checkpoint.", snapshotTimestamp);
        }

        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshotTimestamp)
                .build()
                .begin();
        try (Timer.Context context = MetricsUtils.getConditionalContext(appendCheckpointTimer)) {
            // A checkpoint writer will do two accesses one to obtain the object
            // vlo version and to get a shallow copy of the entry set
            log.info("appendCheckpoint: Started checkpoint for {} at snapshot {}", streamId, snapshotTimestamp);
            ICorfuSMR<T> corfuObject = (ICorfuSMR<T>) this.map;
            Set<Map.Entry> entries = this.map.entrySet();
            // The vloVersion which will determine the checkpoint START_LOG_ADDRESS (last observed update for this
            // stream by the time of checkpointing) is defined by the stream's tail instead of the stream's version,
            // as the latter discards holes for resolution, hence if last address is a hole it would diverge
            // from the stream address space maintained by the sequencer.
            long vloVersion = snapshotTimestamp.getSequence();
            startCheckpoint(snapshotTimestamp, vloVersion);
            appendObjectState(entries);
            finishCheckpoint();
            long cpDuration = System.currentTimeMillis() - start;
            log.info("appendCheckpoint: completed checkpoint for {}, entries({}), tableSize({}) bytes, " +
                            "cpSize({}) bytes at snapshot {} in {} ms",
                    streamId, entries.size(), MetricsUtils.sizeOf.deepSizeOf(entries),
                    numBytes, snapshotTimestamp, cpDuration);
        } finally {
            rt.getObjectsView().TXEnd();
        }

        return snapshotTimestamp;
    }

    private Token forceHole() {
        Token writeToken = rt.getSequencerView().next(streamId).getToken();
        LogData logData = new LogData(DataType.HOLE);
        logData.useToken(writeToken);
        rt.getAddressSpaceView().write(writeToken, logData, CacheOption.WRITE_AROUND);
        return writeToken;
    }

    /** Append a checkpoint START record to this object's stream.
     *
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     * @return Global log address of the START record.
     */
    public void startCheckpoint(Token txnSnapshot, long vloVersion) {
        startTime = LocalDateTime.now();
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_TIME, startTime.toString());
        // VLO version at time of snapshot
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, Long.toString(vloVersion));
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS,
                Long.toString(txnSnapshot.getSequence()));

        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> mdkv =
                ImmutableMap.copyOf(this.mdkv);
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointId, streamId, mdkv, null);
        startAddress = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, startAddress);
    }

    /**
     *  Append an object to a stream without caching the entries.
     */
    private long nonCachedAppend(Object object, UUID ... streamIDs) {
        return sv.append(object, null, CacheOption.WRITE_AROUND, streamIDs);
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
    public void appendObjectState(Set<Map.Entry> entries) {
        ImmutableMap<CheckpointEntry.CheckpointDictKey, String> mdkv =
                ImmutableMap.copyOf(this.mdkv);

        Iterable<List<Map.Entry>> partitions = Iterables.partition(entries, batchSize);

        for (List<Map.Entry> partition : partitions) {
            MultiSMREntry smrEntries = new MultiSMREntry();
            for (Map.Entry entry : partition) {
                smrEntries.addTo(new SMREntry("put",
                        new Object[]{keyMutator.apply(entry.getKey()),
                                valueMutator.apply(entry.getValue())},
                        serializer));
            }

            CheckpointEntry cp = new CheckpointEntry(CheckpointEntry
                    .CheckpointEntryType.CONTINUATION,
                    author, checkpointId, streamId, mdkv, smrEntries);
            long pos = nonCachedAppend(cp, checkpointStreamID);
            postAppendFunc.accept(cp, pos);
            numEntries++;
            // CheckpointEntry::serialize() has a side-effect we use
            // for an accurate count of serialized bytes of SRMEntries.
            numBytes += cp.getSmrEntriesBytes();
        }
    }

    /** Append a checkpoint END record to this object's stream.
     *
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     * @return Global log address of the END record.
     */
    public void finishCheckpoint() {
        LocalDateTime endTime = LocalDateTime.now();
        mdkv.put(CheckpointEntry.CheckpointDictKey.END_TIME, endTime.toString());
        numEntries++;
        numBytes++;
        mdkv.put(CheckpointEntry.CheckpointDictKey.ENTRY_COUNT, Long.toString(numEntries));
        mdkv.put(CheckpointEntry.CheckpointDictKey.BYTE_COUNT, Long.toString(numBytes));

        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                author, checkpointId, streamId, mdkv, null);

        endAddress = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, endAddress);
    }
}
