package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.CacheOption;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;


/** Checkpoint writer for CorfuTables: take a snapshot of the
 *  object via TXBegin(), then dump the frozen object's
 *  state into CheckpointEntry records into the object's
 *  stream.
 *  TODO: Generalize to all SMR objects.
 */
@Slf4j
public class CheckpointWriter<T extends ICorfuTable<?, ?>> {
    /** Metadata to be stored in the CP's 'dict' map.
     */
    private final UUID streamId;
    private final String author;
    private final UUID checkpointId;

    @Getter
    private long numEntries = 0;
    private long numBytes = 0;

    /** Creates a batch that is 95% of the max write size to accommodate for metadata overhead.
     *  Metadata overhead is the size of an empty CheckpointEntry. This parameter also helps to
     *  tune to the number of smr entries per CONTINUATION record of the checkpoint entry.
     */
    @Setter
    @Getter
    private double batchThresholdPercentage = 0.95;
    private static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 * (1 << 20);

    /** Batch size: number of SMREntry in a single CONTINUATION.
     */
    @Getter
    @Setter
    private int batchSize;

    /**
     *  Max uncompressed checkpoint entry size: Maximum uncompressed size of a single Checkpoint CONTINUATION Entry.
     */
    @Getter
    @Setter
    private long maxUncompressedCpEntrySize;

    @SuppressWarnings("checkstyle:abbreviation")
    private final UUID checkpointStreamID;
    private final Map<CheckpointEntry.CheckpointDictKey, String> mdkv = new HashMap<>();

    /** Mutator lambda to change map key.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> keyMutator = x -> x;

    /** Mutator lambda to change map value.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> valueMutator = x -> x;

    /** BiConsumer to run after every CheckpointEntry is appended to the stream.
     */
    @Getter
    @Setter
    BiConsumer<CheckpointEntry,Long> postAppendFunc = (cp, l) -> { };

    /** Local ref to the object's runtime.
     */
    private final CorfuRuntime rt;

    /** Local ref to the stream's view.
     */
    final StreamsView sv;

    /** Local ref to the object that we're dumping.
     *  TODO: generalize to all SMR objects.
     */
    @Getter
    private final T corfuTable;

    @Getter
    @Setter
    ISerializer serializer = Serializers.getDefaultSerializer();

    /** Constructor for Checkpoint Writer for Corfu Maps.
     * @param rt object's runtime
     * @param streamId unique identifier of stream to checkpoint
     * @param author checkpoint initiator
     * @param corfuTable local reference of the PersistentCorfuTable to checkpoint
     */
    public CheckpointWriter(CorfuRuntime rt, UUID streamId, String author, T corfuTable) {
        this.rt = rt;
        this.streamId = streamId;
        this.author = author;
        this.corfuTable = corfuTable;
        checkpointId = UUID.randomUUID();
        checkpointStreamID = CorfuRuntime.getCheckpointStreamIdFromId(streamId);
        sv = rt.getStreamsView();
        maxUncompressedCpEntrySize = rt.getParameters().getMaxUncompressedCpEntrySize();
        batchSize = rt.getParameters().getCheckpointBatchSize();
    }

    /**
     * Append a checkpoint, i.e., coalesce the state of the log. The snapshot
     * of this checkpoint will be internally computed as to the current tail of the log.
     *
     * @return Token at which the snapshot for this checkpoint was taken.
     */
    public Token appendCheckpoint(Optional<LivenessUpdater> livenessUpdater) {
        // We enforce a NO_OP entry for every checkpoint, i.e., a hole with backpointer map info,
        // to materialize the stream up to this point (no future sequencer regression) and in addition ensure
        // log unit address maps reflect the latest update to the stream preventing tail regression in the
        // event of sequencer failover (if for instance the last update to the stream was a hole).
        Token snapshot = forceNoOpEntry();
        return appendCheckpoint(snapshot, livenessUpdater);
    }

    public Token appendCheckpoint() {
        return appendCheckpoint(Optional.empty());
    }

    /**
     * Write a checkpoint which reflects the state at snapshot.
     *
     * This API should not be directly invoked.
     *
     *  @param snapshotTimestamp snapshot at which the checkpoint is taken.
     *  */
    @VisibleForTesting
    public Token appendCheckpoint(Token snapshotTimestamp, Optional<LivenessUpdater> livenessUpdater) {
        long start = System.currentTimeMillis();

        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshotTimestamp)
                .build()
                .begin();

        log.info("appendCheckpoint: Started checkpoint for {} at snapshot {}", streamId, snapshotTimestamp);

        
        try (Stream<? extends Map.Entry<?, ?>> entries = this.corfuTable.entryStream()) {
            // A checkpoint writer will do two accesses one to obtain the object
            // vlo version and to get a shallow copy of the entry set
            // The vloVersion which will determine the checkpoint START_LOG_ADDRESS (last observed update for this
            // stream by the time of checkpointing) is defined by the stream's tail instead of the stream's version,
            // as the latter discards holes for resolution, hence if last address is a hole it would diverge
            // from the stream address space maintained by the sequencer.

            // After the table has been synced, stop updating heartbeat for it.
            // The liveness will start to be checked via checkpoint stream tail.
            livenessUpdater.ifPresent(LivenessUpdater::unsetCurrentTable);

            startCheckpoint(snapshotTimestamp);
            int entryCount = appendObjectState(entries);
            finishCheckpoint();
            long cpDuration = System.currentTimeMillis() - start;
            MicroMeterUtils.time(Duration.ofMillis(cpDuration), "checkpoint.timer",
                    "streamId", streamId.toString());
            MicroMeterUtils.measure(numBytes, "checkpoint.write_size");
            MicroMeterUtils.measure(entryCount, "checkpoint.write_entries");
            log.info("appendCheckpoint: completed checkpoint for {}, entries({}), " +
                            "cpSize({}) bytes at snapshot {} in {} ms",
                    streamId, entryCount, numBytes, snapshotTimestamp, cpDuration);
        } finally {
            rt.getObjectsView().TXEnd();
        }

        return snapshotTimestamp;
    }

    private Set<UUID> discoverTableTags(UUID stream) {
        Set<UUID> tags = new HashSet<>();
        Set<CorfuStoreMetadata.TableName> names = ((DynamicProtobufSerializer) serializer).getCachedRegistryTable().keySet();
        for (CorfuStoreMetadata.TableName tableName : names) {
            String qName = TableRegistry.getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName());
            if (CorfuRuntime.getStreamID(qName).equals(stream)) {
                ((DynamicProtobufSerializer) serializer)
                        .getCachedRegistryTable()
                        .get(tableName)
                        .getMetadata().getTableOptions().getStreamTagList()
                        .forEach(tagName -> tags.add(TableRegistry.getStreamIdForStreamTag(tableName.getNamespace(), tagName)));

                break;
            }
        }
        return tags;
    }

    public Token forceNoOpEntry() {
        Set<UUID> streamsToAdvance = new HashSet<>();
        if (serializer instanceof DynamicProtobufSerializer) {
            // One of the use-cases of a no-op entry written by the checkpointer is to always - even for empty streams -
            // have a written address per-stream after a prefix trim. This is required for stream implementations to
            // detect whether there is a trim gap while syncing. For example, if the last read address read by the
            // stream is less than the stream's trim mark then a trim gap has occurred and a stream must re-sync.
            // Since UFO stream listeners can track multiple table updates via stream tags, we need to also
            // make sure that each stream tag has a valid address after a prefix trim. In addition to the table
            // stream, discoverTableTags will find all tags associated with a table and also write a no-op on
            // their streams.
            streamsToAdvance.addAll(discoverTableTags(streamId));
        }

        streamsToAdvance.add(streamId);
        TokenResponse writeToken = rt.getSequencerView()
                .next(streamsToAdvance.toArray(new UUID[streamsToAdvance.size()]));
        LogData logData = new LogData(DataType.HOLE);
        rt.getAddressSpaceView().write(writeToken, logData, CacheOption.WRITE_AROUND);
        return writeToken.getToken();
    }

    /** Append a checkpoint START record to this object's stream.
     *
     *  <p>Corfu client transaction management, if desired, is the
     *  caller's responsibility.</p>
     *
     * @return Global log address of the START record.
     */
    public void startCheckpoint(Token txnSnapshot) {
        long vloVersion = txnSnapshot.getSequence();
        LocalDateTime startTime = LocalDateTime.now();
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_TIME, startTime.toString());
        // VLO version at time of snapshot
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, Long.toString(vloVersion));
        this.mdkv.put(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS,
                Long.toString(txnSnapshot.getSequence()));

        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> kvCopy =
                ImmutableMap.copyOf(this.mdkv);
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointId, streamId, kvCopy, null);
        long startAddress = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, startAddress);
    }

    /**
     *  Append an object to a stream without caching the entries.
     */
    private long nonCachedAppend(Object object, UUID ... streamIDs) {
        return sv.append(object, null, CacheOption.WRITE_AROUND, true, streamIDs);
    }

    /**
     * Create a checkpoint continuation record from a MultiSMREntry
     */
    private void convertAndAppendCheckpointEntry(MultiSMREntry smrEntries,
                                                 Map<CheckpointEntry.CheckpointDictKey,String> kvCopy) {
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry
                .CheckpointEntryType.CONTINUATION,
                author, checkpointId, streamId, kvCopy, smrEntries);
        long pos = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, pos);
        numEntries++;
        // CheckpointEntry::serialize() has a side-effect we use
        // for an accurate count of serialized bytes of SRMEntries.
        numBytes += cp.getSmrEntriesBytes();
    }

    /** Append zero or more CONTINUATION records to this
     *  object's stream.  Each will contain a fraction of
     *  the state of the object that we're checkpointing
     *  ( adding items one a time before reaching BATCH_THRESHOLD *
     *  maxWriteSize for log entries boundary OR up to batchSize items
     *  at a time for testing purposes).
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
    public int appendObjectState(Stream<? extends Map.Entry<?, ?>> entryStream) {
        int maxWriteSizeLimit = (int) (batchThresholdPercentage * getMaxWriteSize());
        ImmutableMap<CheckpointEntry.CheckpointDictKey,String> kvCopy =
                ImmutableMap.copyOf(this.mdkv);

        int totalEntryCount = 0;
        int numBytesPerCheckpointEntry = 0;
        int numBytesPerUncompressedCheckpointEntry = 0;

        MultiSMREntry smrEntries = new MultiSMREntry();

        Iterator<? extends Map.Entry<?, ?>> iterator = entryStream.iterator();
        while (iterator.hasNext()) {
            Map.Entry<?, ?> entry = iterator.next();
            SMREntry smrPutEntry = new SMREntry("put",
                    new Object[]{keyMutator.apply(entry.getKey()),
                            valueMutator.apply(entry.getValue())},
                    serializer);

            /* Need to check the size of the compressed buffer. inputByteBuffer and
               inputBuffer are the same buffer.
             */
            ByteBuf inputBuffer = Unpooled.buffer();
            smrPutEntry.serialize(inputBuffer);
            numBytesPerUncompressedCheckpointEntry += smrPutEntry.getSerializedSize();
            int compressedBufferSize = getCompressedBufferSize(inputBuffer);
            numBytesPerCheckpointEntry += compressedBufferSize;
            inputBuffer.clear();

            /* CheckpointEntry has some metadata and make the total size larger than the actual size
             * of SMR entries. Its a safeguard against the smr entries amounting to the actual
             * boundary limit.
             */
            if (numBytesPerUncompressedCheckpointEntry > maxUncompressedCpEntrySize
                    || numBytesPerCheckpointEntry > maxWriteSizeLimit || smrEntries.getUpdates().size() >= batchSize) {
                convertAndAppendCheckpointEntry(smrEntries, kvCopy);
                log.trace("Batched size of checkpoint log entry consists {} smr entries",
                        smrEntries.getUpdates().size());
                /* reset the num of bytes and the new batch size entries below
                also, reset the smr entry to add the newly read SMR:each from the stream. */
                numBytesPerCheckpointEntry = compressedBufferSize;
                numBytesPerUncompressedCheckpointEntry = smrPutEntry.getSerializedSize();
                smrEntries = new MultiSMREntry();
            }
            smrEntries.addTo(smrPutEntry);
            // maintain current batch size only for test purposes.
            totalEntryCount++;
        }

        // the entries which are left behind for a final flush.
        if (!smrEntries.getUpdates().isEmpty()) {
            convertAndAppendCheckpointEntry(smrEntries, kvCopy);
            log.trace("Final checkpoint log entry consists {} smr entries",
                    smrEntries.getUpdates().size());
        }
        return totalEntryCount;
    }

    private int getCompressedBufferSize(ByteBuf inputBuffer) {
        ByteBuffer compressedBuffer =
                rt.getParameters()
                        .getCodecType()
                        .getInstance().compress(ByteBuffer.wrap(inputBuffer.array(),
                                inputBuffer.readerIndex(), inputBuffer.writerIndex()));
        return compressedBuffer.limit();
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

        long endAddress = nonCachedAppend(cp, checkpointStreamID);

        postAppendFunc.accept(cp, endAddress);
    }

    private int getMaxWriteSize() {
        int maxWriteSize = rt.getParameters().getMaxWriteSize();
        return maxWriteSize == Integer.MAX_VALUE ? DEFAULT_CP_MAX_WRITE_SIZE : maxWriteSize;
    }
}
