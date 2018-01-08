package org.corfudb.recovery;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import static org.corfudb.recovery.RecoveryUtils.*;
import static org.corfudb.runtime.view.Address.isAddress;

/** The FastObjectLoader reconstructs the coalesced state of SMRMaps through sequential log read
 *
 * This utility reads Log entries sequentially extracting the SMRUpdates from each entry
 * and build the Maps as we go. In the presence of checkpoints, the checkpoint entries will
 * be applied before the normal entries starting after the checkpoint start address.
 *
 * If used in the recoverSequencer mode, it will reconstruct the stream tails.
 *
 * There are two main modes, blacklist and whitelist. These two modes are mutually exclusive:
 * In blacklist mode, we will process every streams as long as they are not in the streamToIgnore
 * list. In whitelist mode, only the streams present in streamsToLoad will be loaded. We make
 * sure to also include the checkpoint streams for each of them.
 *
 *
 * Created by rmichoud on 6/14/17.
 */

@Slf4j
@Accessors(chain = true)
public class FastObjectLoader {

    static final long DEFAULT_BATCH_FOR_FAST_LOADER = 10;
    static final int DEFAULT_TIMEOUT_MINUTES_FAST_LOADING = 30;
    static final int NUMBER_OF_ATTEMPT = 3;
    static final int STATUS_UPDATE_PACE = 10000;

    private CorfuRuntime runtime;

    @Setter
    @Getter
    Class defaultObjectsType = SMRMap.class;

    @Setter
    @Getter
    private boolean loadInCache;

    @Setter
    @Getter
    private long logHead = -1;

    @Setter
    @Getter
    private long logTail = -1;

    @Setter
    @Getter
    private long batchReadSize = DEFAULT_BATCH_FOR_FAST_LOADER;

    @Setter
    @Getter
    private int timeoutInMinutesForLoading = DEFAULT_TIMEOUT_MINUTES_FAST_LOADING;

    @Setter
    @Getter
    private boolean recoverSequencerMode;

    @Setter
    @Getter
    private boolean logHasNoCheckPoint = false;

    private boolean whiteList = false;
    private List<UUID> streamsToLoad = new ArrayList<>();

    /**
     * Enable whiteList mode where we only reconstruct
     * the streams provided through this api. In this mode,
     * we will only process the streams present in streamsToLoad.
     * All the others are ignored.
     * @param streamsToLoad
     */
    public FastObjectLoader addStreamsToLoad(List<String> streamsToLoad) {
        if (streamsToIgnore.size() != 0) {
            throw new IllegalStateException("Cannot add a whitelist when there are already streams to ignore");
        }

        whiteList = true;

        streamsToLoad.forEach(streamName -> {
            this.streamsToLoad.add(CorfuRuntime.getStreamID(streamName));
            // Generate the streamsCheckpointId (we need to allow them as well)
            this.streamsToLoad.add(CorfuRuntime.getCheckpointStreamIdFromName(streamName));
        });

        return this;
    }

    /**
     * We can add streams to be ignored during the
     * reconstruction of the state (e.g. raw streams)
     */
    @Getter
    private Set<UUID> streamsToIgnore = new HashSet<>();

    /**
     * We can register streams with non-default type
     */
    private Map<UUID, ObjectBuilder> customTypeStreams = new HashMap<>();

    public void addCustomTypeStream(UUID streamId, ObjectBuilder ob) {
        customTypeStreams.put(streamId, ob);
    }

    /**
     * Add an indexer to a stream (that backs a CorfuTable)
     * @param streamName
     * @param indexer
     */
    public void addIndexerToCorfuTableStream(String streamName, Class<?> indexer) {
        UUID streamId = CorfuRuntime.getStreamID(streamName);
        ObjectBuilder ob = new ObjectBuilder(runtime).setType(CorfuTable.class)
                .setArguments(indexer).setStreamID(streamId);
        addCustomTypeStream(streamId, ob);
    }

    private Class getStreamType(UUID streamId) {
        if (customTypeStreams.containsKey(streamId)) {
            return customTypeStreams.get(streamId).getType();
        }

        return defaultObjectsType;
    }

    private long addressProcessed;

    // In charge of summoning Corfu maps back in this world
    private ExecutorService necromancer;

    private Map<UUID, StreamMetaData> streamsMetaData;

    @Getter
    private Map<UUID, Long> streamTails = new HashMap<>();

    @Setter
    @Getter
    private int numberOfAttempt = NUMBER_OF_ATTEMPT;

    private int retryIteration = 0;
    private long nextRead;

    private List<Future> futureList;

    public FastObjectLoader(@Nonnull final CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        loadInCache = !corfuRuntime.getParameters().isCacheDisabled();
        streamsMetaData = new HashMap<>();
    }

    public void addStreamToIgnore(String streamName) {
        // In a whitelist mode, we cannot add streams to the blacklist
        if (whiteList) {
            throw new IllegalStateException("Cannot add a stream to the blacklist (streamsToIgnore)" +
                    "in whitelist mode.");
        }

        streamsToIgnore.add(CorfuRuntime.getStreamID(streamName));
        // Ignore also checkpoint of this stream
        streamsToIgnore.add(CorfuRuntime.getCheckpointStreamIdFromName(streamName));
    }

    /**
     * Necromancer Utilities
     *
     * Necromancy is a supposed practice of magic involving communication with the deceased
     * – either by summoning their spirit as an apparition or raising them bodily. This suits
     * what this thread is tasked with, bringing back the SMR Maps from their grave (the Log).
     *
     */
    private void summonNecromancer() {
        necromancer = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("necromancer-%d").build());
        futureList = new ArrayList<>();
    }

    private void invokeNecromancer(Map<Long, ILogData> logDataMap, BiConsumer<Long, ILogData> resurrectionSpell) {
        futureList.add(necromancer.submit(() -> {
            logDataMap.forEach((address, logData) -> {
                resurrectionSpell.accept(address, logData);
            });
        }));
    }

    private void killNecromancer() {
        necromancer.shutdown();
        try {
            necromancer.awaitTermination(timeoutInMinutesForLoading, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            String msg = "Necromancer is taking too long to load the maps. Gave up.";
            log.error(msg);
            fail(msg);
        }
        for (Future future : futureList) {
            CFUtils.getUninterruptibly(future);

        }
    }

    /**
     * These two functions are called if no parameter were supplied
     * by the user.
     */
    private void findAndSetLogHead() {
         logHead = runtime.getAddressSpaceView().getTrimMark();
    }

    private void findAndSetLogTail() {
        logTail = runtime.getSequencerView().nextToken(Collections.emptySet(), 0).getTokenValue();
    }

    private void resetAddressProcessed() {
        addressProcessed = logHead - 1;
    }

    /**
     * Book keeping of the the stream tails
     *
     * @param logData
     */
    public void updateStreamTails(long address, ILogData logData) {
        // On checkpoint, we also need to track the stream tail of the checkpoint
        if (isCheckPointEntry(logData)) {
            if (logData.getCheckpointType() == CheckpointEntry.CheckpointEntryType.END &&
                    isAddress(getStartAddressOfCheckPoint(logData))) {
                streamTails.compute(logData.getCheckpointedStreamId(),
                        (uuid, value) -> (value == null) ? getStartAddressOfCheckPoint(logData)
                            : Math.max(value, getStartAddressOfCheckPoint(logData)));
            }
        }
        for (UUID streamId : logData.getStreams()) {
            streamTails.compute(streamId,
                    (uuid, value) -> (value == null) ? address : Math.max(value, address));
        }
    }

    /**
     * It is a valid state to have entries that are checkpointed but the log was not fully
     * trimmed yet. This means that some entries are at the same time in their own slot and in
     * the checkpoint. We must avoid to process them twice.
     *
     * This comes especially relevant when the operation order affects the end result.
     * (e.g. clear() operation)
     *
     * @param streamId stream id to validate
     * @param entry entry under scrutinization.
     * @return if the entry is already part of the checkpoint we started from.
     */
    private boolean entryAlreadyContainedInCheckpoint(UUID streamId, SMREntry entry) {
        return streamsMetaData.containsKey(streamId) && entry.getEntry().getGlobalAddress() <
                streamsMetaData.get(streamId).getHeadAddress();
    }

    /**
     * Check if this entry is relevant
     *
     * There are 3 cases where an entry should not be processed:
     *   1. In whitelist mode, if the stream is not in the whitelist (streamToLoad)
     *   2. In blacklist mode, if the stream is in the blacklist (streamToIgnore)
     *   3. If the entry was already processed in the previous checkpoint.
     *
     * @param streamId identifies the Corfu stream
     * @param entry entry to potentially apply
     * @return if we need to apply the entry.
     */
    private boolean shouldEntryBeApplied(UUID streamId, SMREntry entry, boolean isCheckpointEntry) {
        // 1.
        // In white list mode, ignore everything that is not in the list (the list contains the streams
        // passed by the client + derived checkpoint streams).
        if (whiteList && !streamsToLoad.contains(streamId)) {
            return false;
        }

        // 2.
        // We ignore the transaction stream ID because it is a raw stream
        // We don't want to create a Map for it.
        if (streamId == runtime.getObjectsView().TRANSACTION_STREAM_ID || streamsToIgnore.contains(streamId)) {
            return false;
        }

        // 3.
        // If the entry was already processed with the previous checkpoint.
        if (!isCheckpointEntry && entryAlreadyContainedInCheckpoint(streamId, entry)) {
            return false;
        }

        return true;
    }

    private boolean shouldStreamBeProcessed(UUID streamId) {
        if (whiteList) {
            return streamsToLoad.contains(streamId);
        }

        return !streamsToIgnore.contains(streamId);
    }

    /**
     * If none of the streams in the logData should be processed, we
     * can simply ignore this logData.
     *
     * In the case of a mix of streams we need to process and other that we don't,
     * we will still go ahead with the process.
     *
     * @param logData
     * @return
     */
    private boolean shouldLogDataBeProcessed(ILogData logData) {
        boolean shouldProcess = false;
        for (UUID id : logData.getStreams()) {
            if (shouldStreamBeProcessed(id)){
                shouldProcess = true;
            }
        }
        return shouldProcess;
    }


    /**
     * Update the corfu object and it's underlying stream with the new entry.
     *
     * @param streamId identifies the Corfu stream
     * @param entry entry to apply
     * @param globalAddress global address of the entry
     * @param isCheckPointEntry
     */
    private void applySmrEntryToStream(UUID streamId, SMREntry entry,
                                       long globalAddress, boolean isCheckPointEntry) {
        if (shouldEntryBeApplied(streamId, entry, isCheckPointEntry)) {

            // Get the serializer type from the entry
            ISerializer serializer = Serializers.getSerializer(entry.getSerializerType().getType());

            // Get the type of the object we want to recreate
            Class objectType = getStreamType(streamId);

            // Create an Object only for non-checkpoints

            // If it is a special type, create it with the object builder
            if (customTypeStreams.containsKey(streamId)) {
                createObjectIfNotExist(customTypeStreams.get(streamId), serializer);
            }
            else {
                createObjectIfNotExist(runtime, streamId, serializer, objectType);
            }
            CorfuCompileProxy cp = getCorfuCompileProxy(runtime, streamId, objectType);
            cp.getUnderlyingObject().applyUpdateToStreamUnsafe(entry, globalAddress);
        }
    }

    private void applySmrEntryToStream(UUID streamId, SMREntry entry, long globalAddress) {
        applySmrEntryToStream(streamId, entry, globalAddress, false);

    }


    private void updateCorfuObjectWithSmrEntry(ILogData logData, LogEntry logEntry, long globalAddress) {
        UUID streamId = logData.getStreams().iterator().next();
        applySmrEntryToStream(streamId, (SMREntry) logEntry, globalAddress);
    }

    private void updateCorfuObjectWithMultiObjSmrEntry(LogEntry logEntry, long globalAddress) {
        MultiObjectSMREntry multiObjectLogEntry = (MultiObjectSMREntry) logEntry;
        multiObjectLogEntry.getEntryMap().forEach((streamId, multiSmrEntry) -> {
            multiSmrEntry.getSMRUpdates(streamId).forEach((smrEntry) -> {
                applySmrEntryToStream(streamId, smrEntry, globalAddress);
            });
        });
    }

    private void updateCorfuObjectWithCheckPointEntry(ILogData logData, LogEntry logEntry) {
        CheckpointEntry checkPointEntry = (CheckpointEntry) logEntry;
        // Just one stream, always
        UUID streamId = checkPointEntry.getStreamId();
        UUID checkPointId = checkPointEntry.getCheckpointId();

        // We need to apply the start address for the version of the object
        long startAddress = streamsMetaData.get(streamId)
                .getCheckPoint(checkPointId)
                .getStartAddress();

        // We don't know in advance if there will be smrEntries
        if (checkPointEntry.getSmrEntries() != null) {
            checkPointEntry.getSmrEntries().getSMRUpdates(streamId).forEach((smrEntry) -> {
                applySmrEntryToStream(checkPointEntry.getStreamId(), smrEntry,
                        startAddress, true);
            });
        }
    }

    /**
     * Extract log entries from logData and update the Corfu Objects
     *
     * @param logData LogData received from Corfu server.
     */
    private void updateCorfuObject(ILogData logData) {
        LogEntry logEntry;
        try {
            logEntry = deserializeLogData(runtime, logData);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (Exception e) {
            log.error("Cannot deserialize log entry" + logData.getGlobalAddress(), e);
            return;
        }

        long globalAddress = logData.getGlobalAddress();

        switch (logEntry.getType()) {
            case SMR:
                updateCorfuObjectWithSmrEntry(logData, logEntry, globalAddress);
                break;
            case MULTIOBJSMR:
                updateCorfuObjectWithMultiObjSmrEntry(logEntry, globalAddress);
                break;
            case CHECKPOINT:
                updateCorfuObjectWithCheckPointEntry(logData, logEntry);
                break;
            default:
                log.warn("updateCorfuObject[address = {}]: Unknown data type");

        }
    }


    /**
     * Initialize log head and log tails
     *
     * If logHead and logTail has not been initialized by
     * the user, initialize to default.
     *
     */
    private void initializeHeadAndTails() {
        if (logHead < 0) {
            findAndSetLogHead();
        }

        if (logTail < 0) {
            findAndSetLogTail();
        }

        resetAddressProcessed();
    }

    private void cleanUpForRetry() {
        runtime.getAddressSpaceView().invalidateClientCache();
        runtime.getObjectsView().getObjectCache().clear();
        streamTails.clear();
        nextRead = logHead;
        resetAddressProcessed();

        // Re ask for the Head, if it changes while we were trying
        findAndSetLogHead();
    }
    /**
     * Increment the retry iteration.
     *
     * If we reached the max number of entry, throw a Runtime Exception.
     */
    private void handleRetry() {

        retryIteration++;
        if (retryIteration > numberOfAttempt) {
            log.error("processLogData[]: retried {} number of times and failed", retryIteration);
            throw new RuntimeException("FastObjectLoader failed after too many retry (" + retryIteration + ")");
        }
        else {
            cleanUpForRetry();
        }
    }

    /**
     * Fail the FastObjectLoader throwing a RuntimeException
     *
     * @param msg message passed in the RuntimeException
     */
    private void fail(String msg) {
        throw new RuntimeException(msg);
    }

    /**
     * Dispatch logData given it's type
     *
     * @param address
     * @param logData
     */
    private void processLogData(long address, ILogData logData) {
        switch (logData.getType()) {
            case DATA:
                // Checkpoint should have been processed first
                if (!isCheckPointEntry(logData) && shouldLogDataBeProcessed(logData)) {
                    updateCorfuObject(logData);
                }
                break;
            case HOLE:
                break;
            case TRIMMED:
                break;
            case EMPTY:
                log.warn("applyForEachAddress[address={}] is empty");
                break;
            case RANK_ONLY:
                break;
            default:
                break;
        }
    }

    /**
     * When we encounter a start checkpoint, we need to create the new entry in the Stream
     * @param address
     * @param logData
     * @param streamId
     * @param checkPointId
     * @param streamMeta
     */
    private void handleStartCheckPoint(long address, ILogData logData, UUID streamId,
                                       UUID checkPointId, StreamMetaData streamMeta) {
        try {
            CheckpointEntry logEntry = (CheckpointEntry) deserializeLogData(runtime, logData);
            long snapshotAddress = getSnapShotAddressOfCheckPoint(logEntry);
            long startAddress = getStartAddressOfCheckPoint(logData);

            streamMeta.addCheckPoint(new CheckPoint(checkPointId)
                    .addAddress(address)
                    .setSnapshotAddress(snapshotAddress)
                    .setStartAddress(startAddress)
                    .setStarted(true));

        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (Exception e) {
            log.error("findCheckpointsInLogAddress[{}]: "
                    + "Couldn't get the snapshotAddress", address, e);
            fail("Couldn't get the snapshotAddress at address " + address);
        }
    }

    /**
     * Find if there is a checkpoint in the current logAddress
     *
     * If there is a checkpoint, the streamsMetadata map will be
     * updated accordingly.
     *
     * We will only use the first checkpoint
     *
     * @param address
     * @param logData
     */
    private void findCheckPointsInLogAddress(long address, ILogData logData) {
        if (logData.hasCheckpointMetadata() &&
                shouldLogDataBeProcessed(logData)) {
            // Only one stream per checkpoint
            UUID streamId = logData.getCheckpointedStreamId();
            StreamMetaData streamMeta;
            streamMeta = streamsMetaData.computeIfAbsent(streamId, (id) -> new StreamMetaData(id));
            UUID checkPointId = logData.getCheckpointId();

            switch (logData.getCheckpointType()) {
                case START:
                    handleStartCheckPoint(address, logData, streamId, checkPointId, streamMeta);

                    break;
                case CONTINUATION:
                    if (streamMeta.checkPointExists(checkPointId)) {
                        streamMeta.getCheckPoint(checkPointId).addAddress(address);
                    }

                    break;
                case END:
                    if (streamMeta.checkPointExists(checkPointId)) {
                        streamMeta.getCheckPoint(checkPointId).setEnded(true).addAddress(address);
                        streamMeta.updateLatestCheckpointIfLater(checkPointId);
                    }
                    break;
                default:
                    log.warn("findCheckPointsInLog[address = {}] Unknown checkpoint type", address);
                    break;
            }
        }
    }


    /**
     * Apply the checkPoints in parallel
     *
     * Since each checkpoint is mapped to a single stream, we can parallelize
     * this operation.
     *
     */
    private void resurrectCheckpoints() {
        streamsMetaData.entrySet().parallelStream()
                .forEach(entry -> {
                    CheckPoint checkPoint = entry.getValue().getLatestCheckPoint();
                    if (checkPoint == null) {
                        log.info("resurrectCheckpoints[{}]: Truncated checkpoint for this stream",
                                Utils.toReadableId(entry.getKey()));
                        return;
                    }

                    // For now one by one read and apply
                    for (long address : checkPoint.getAddresses()) {
                        updateCorfuObject(getLogData(runtime, loadInCache, address));
                    }
                });
    }

    /**
     * This method will only resurrect the stream tails. It is used
     * to recover a sequencer.
     */
    private void recoverSequencer() {
        log.info("recoverSequencer: Resurrecting the stream tails");
        applyForEachAddress(this::updateStreamTails);
    }

    /**
     * This method will use the checkpoints and the entries
     * after checkpoints to resurrect the SMRMaps
     */
    private void recoverRuntime() {
        log.info("recoverRuntime: Resurrecting the runtime");

        // If the user is sure that he has no checkpoint,
        // we can just do the last step. Risky, but the flag is
        // explicit enough.
        if (logHasNoCheckPoint) {
            applyForEachAddress(this::processLogData);
        } else {
            applyForEachAddress(this::findCheckPointsInLogAddress);
            resurrectCheckpoints();

            resetAddressProcessed();
            applyForEachAddress(this::processLogData);
        }

    }

    /**
     * Entry point to load the SMRMaps in memory.
     *
     * When this function returns, the maps are fully loaded.
     */
    public void loadMaps() {
        log.info("loadMaps: Starting to resurrect maps");
        initializeHeadAndTails();

        if(recoverSequencerMode) {
            recoverSequencer();
        }
        else {
            recoverRuntime();
        }

        log.info("loadMaps[startAddress: {}, stopAddress (included): {}, addressProcessed: {}]",
                logHead, logTail, addressProcessed);
        log.info("loadMaps: Loading successful, Corfu maps are alive!");
    }


    /**
     * This method will apply for each address the consumer given in parameter.
     *
     * The Necromancer thread is used to do the heavy lifting.
     * @param logDataProcessor
     */
    private void applyForEachAddress(BiConsumer<Long, ILogData> logDataProcessor) {

        summonNecromancer();
        nextRead = logHead;
        while (nextRead <= logTail) {
            final long start = nextRead;
            final long stopNotIncluded = Math.min(start + batchReadSize, logTail + 1);
            nextRead = stopNotIncluded;
            final Map<Long, ILogData> range = getLogData(runtime, start, stopNotIncluded);

            // Sanity
            boolean canProcessRange = true;
            for(Map.Entry<Long, ILogData> entry : range.entrySet()) {
                long address = entry.getKey();
                ILogData logData = entry.getValue();
                if (address != addressProcessed + 1) {
                    fail("We missed an entry. It can lead to correctness issues.");
                }
                addressProcessed++;

                if (logData.getType() == DataType.TRIMMED) {
                    log.warn("applyForEachAddress[{}, start={}] address is trimmed", address, logHead);
                    handleRetry();
                    canProcessRange = false;
                    break;
                }

                if(address % STATUS_UPDATE_PACE == 0) {
                    log.info("applyForEachAddress: read up to {}", address);
                }
            }
            if (canProcessRange) {
                invokeNecromancer(range, logDataProcessor);
            }
        }
        killNecromancer();
    }

    @Data
    private class CheckPoint {
        final UUID checkPointId;
        long snapshotAddress;
        long startAddress;
        boolean ended = false;
        boolean started = false;
        List<Long> addresses = new ArrayList<>();

        public CheckPoint addAddress(long address) {
            addresses.add(address);
            return this;
        }
    }

    @Data
    private class StreamMetaData {
        final UUID streamId;
        CheckPoint latestCheckPoint;
        Map<UUID, CheckPoint> checkPoints = new HashMap<>();

        public Long getHeadAddress() {
            return latestCheckPoint != null ? latestCheckPoint.snapshotAddress : Address.NEVER_READ;
        }

        public void addCheckPoint(CheckPoint cp) {
            checkPoints.put(cp.getCheckPointId(), cp);
        }

        public CheckPoint getCheckPoint(UUID checkPointId) {
            return checkPoints.get(checkPointId);
        }

        public boolean checkPointExists(UUID checkPointId) {
            return checkPoints.containsKey(checkPointId);
        }

        public void updateLatestCheckpointIfLater(UUID checkPointId) {
            CheckPoint contender = getCheckPoint(checkPointId);
            if (latestCheckPoint == null ||
                    contender.getSnapshotAddress() > latestCheckPoint.getSnapshotAddress()) {
                        latestCheckPoint = contender;
            }
        }
    }
}

