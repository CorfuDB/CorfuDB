package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.CheckpointException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.CacheOption;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;


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
    private AtomicLong numEntries = new AtomicLong(0);
    private AtomicLong numBytes = new AtomicLong(0);

    // Registry and Timer used for measuring append checkpoint
    private static MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
    private static final String CHECKPOINT_TIMER_NAME = CorfuComponent.GARBAGE_COLLECTION +
            "append-checkpoint";
    private Timer appendCheckpointTimer = metricRegistry.timer(CHECKPOINT_TIMER_NAME);

    @SuppressWarnings("checkstyle:abbreviation")
    final UUID checkpointStreamID;

    Map<CheckpointEntry.CheckpointDictKey, String> mdkv = new HashMap<>();

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

    /**
     * Number of threads to write a singe checkpoint
     */
    @Getter
    @Setter
    int numCPThreads = 4;

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
     * @return List of global addresses of all entries for this checkpoint.
     */
    public Token appendCheckpoint() {
        long start = System.currentTimeMillis();
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();
        try (Timer.Context context = MetricsUtils.getConditionalContext(appendCheckpointTimer)) {
            Token snapshot = TransactionalContext.getCurrentContext().getSnapshotTimestamp();
            // A checkpoint writer will do two accesses one to obtain the object
            // vlo version and to get a shallow copy of the entry set
            log.info("appendCheckpoint: Started checkpoint for {} at snapshot {}", streamId, snapshot);
            ICorfuSMR<T> corfuObject = (ICorfuSMR<T>) this.map;
            Set<Map.Entry> entries = this.map.entrySet();
            long vloVersion = corfuObject.getCorfuSMRProxy().getVersion();
            startCheckpoint(snapshot, vloVersion);
            appendObjectState(entries);
            finishCheckpoint();
            log.info("appendCheckpoint: completed checkpoint for {} at snapshot {} in {} ms", streamId, snapshot,
                    System.currentTimeMillis() - start);
            return snapshot;
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

        ExecutorService executorService = Executors.newFixedThreadPool(numCPThreads,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("checkpointingThreads-%d").build());

        ImmutableMap<CheckpointEntry.CheckpointDictKey, String> mdkv =
                ImmutableMap.copyOf(this.mdkv);

        Iterable<List<Map.Entry>> partitions = Iterables.partition(entries, batchSize);

        log.info("appendObjectState: Checkpoint {}, num of entries {}, table {}",
                checkpointId, entries.size(), streamId);

        List<CompletableFuture> futures = new ArrayList<>();

        try {

            for (List<Map.Entry> partition : partitions) {
                MultiSMREntry smrEntries = new MultiSMREntry();
                for (Map.Entry entry : partition) {
                    smrEntries.addTo(new SMREntry("put",
                            new Object[]{entry.getKey(), entry.getValue()}, serializer));
                }

                CheckpointEntry cp = new CheckpointEntry(CheckpointEntry
                        .CheckpointEntryType.CONTINUATION,
                        author, checkpointId, streamId, mdkv, smrEntries);
                futures.add(CompletableFuture.runAsync(() -> submitWrite(cp, checkpointStreamID),
                        executorService));
            }


            for (CompletableFuture cf : futures) {
                cf.get();
            }
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new CheckpointException(ie);
        } catch(ExecutionException ee) {
            log.error("appendObjectState: encountered an exception while checkpointing {}", streamId, ee);
            throw new CheckpointException(ee.getCause());
        } finally {
            executorService.shutdownNow();
        }
    }

    private void submitWrite(CheckpointEntry ce, UUID checkpointStreamID) {
        long pos = nonCachedAppend(ce, checkpointStreamID);
        numEntries.incrementAndGet();
        // CheckpointEntry::serialize() has a side-effect we use
        // for an accurate count of serialized bytes of SRMEntries.
        numBytes.addAndGet(ce.getSmrEntriesBytes());
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
        numEntries.incrementAndGet();
        numBytes.incrementAndGet();
        mdkv.put(CheckpointEntry.CheckpointDictKey.ENTRY_COUNT, Long.toString(numEntries.get()));
        mdkv.put(CheckpointEntry.CheckpointDictKey.BYTE_COUNT, Long.toString(numBytes.get()));

        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                author, checkpointId, streamId, mdkv, null);

        endAddress = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, endAddress);
    }
}
