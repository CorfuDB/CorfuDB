package org.corfudb.recovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTable.IndexRegistry;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.FastObjectLoaderException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.corfudb.recovery.RecoveryUtils.createObjectIfNotExist;
import static org.corfudb.recovery.RecoveryUtils.deserializeLogData;
import static org.corfudb.recovery.RecoveryUtils.getCorfuCompileProxy;

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
    static final int DEFAULT_NUMBER_OF_PENDING_FUTURES = 1_000;
    static final int DEFAULT_NUMBER_OF_WORKERS = 4;

    private final CorfuRuntime runtime;

    @Setter
    @Getter
    Class defaultObjectsType = SMRMap.class;

    @Setter
    @Getter
    private boolean loadInCache;

    @Setter
    @Getter
    int numberOfPendingFutures = DEFAULT_NUMBER_OF_PENDING_FUTURES;

    /**
     * The number of threads to use for various operations
     * related to building checkpoints and reading addresspace
     * segmenents
     */
    @Getter
    @Setter
    int numOfWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @Getter
    private long logHead = Address.NON_EXIST;

    @Getter
    private long logTail = Address.NON_EXIST;

    @Setter
    @Getter
    private long batchReadSize = DEFAULT_BATCH_FOR_FAST_LOADER;

    @Setter
    @Getter
    private int timeoutInMinutesForLoading = DEFAULT_TIMEOUT_MINUTES_FAST_LOADING;

    private boolean whiteList = false;
    private final List<UUID> streamsToLoad = new ArrayList<>();

    @VisibleForTesting
    void setLogHead(long head) { this.logHead = head; }

    @VisibleForTesting
    void setLogTail(long tail) { this.logTail = tail; }

    // A future to track the last submitted read request
    volatile private Future lastReadRequest;

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
    private final Map<UUID, ObjectBuilder> customTypeStreams = new HashMap<>();

    public void addCustomTypeStream(UUID streamId, ObjectBuilder ob) {
        customTypeStreams.put(streamId, ob);
    }

    /**
     * Add an indexer to a stream (that backs a CorfuTable)
     *
     * @param streamName    Stream name.
     * @param indexRegistry Index Registry.
     */
    public void addIndexerToCorfuTableStream(String streamName, IndexRegistry indexRegistry) {
        UUID streamId = CorfuRuntime.getStreamID(streamName);
        ObjectBuilder ob = new ObjectBuilder(runtime).setType(CorfuTable.class)
                .setArguments(indexRegistry).setStreamID(streamId);
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

    @Setter
    @Getter
    private int numberOfAttempt = NUMBER_OF_ATTEMPT;

    private int retryIteration = 0;
    private long nextRead;

    public FastObjectLoader(@Nonnull final CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        loadInCache = !corfuRuntime.getParameters().isCacheDisabled();
    }

    public void addStreamToIgnore(String streamName) {
        // In a whitelist mode, we cannot add streams to the blacklist
        if (whiteList) {
            throw new IllegalStateException("Cannot add a stream to the blacklist (streamsToIgnore)" +
                    "in whitelist mode.");
        }

        streamsToIgnore.add(CorfuRuntime.getStreamID(streamName));
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
        // Note that the queue implementation requires the corePoolSize to
        // be equal to maximumPoolSize, so this should be fine for a single threaded
        // executor
        necromancer = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new BoundedQueue<>(numberOfPendingFutures),
                new ThreadFactoryBuilder()
                        .setNameFormat("FastObjectLoaderReaderThread-%d").build());
        lastReadRequest = null;
    }

    private void invokeNecromancer(Map<Long, ILogData> logDataMap, BiConsumer<Long, ILogData> resurrectionSpell) {
        lastReadRequest = necromancer.submit(() ->
        {
            logDataMap.forEach((address, logData) -> {
                if (address % STATUS_UPDATE_PACE == 0) {
                    log.info("applyForEachAddress: read up to {}", address);
                }
                resurrectionSpell.accept(address, logData);
            });
        });
    }

    private void killNecromancer() {
        necromancer.shutdown();
        try {
            necromancer.awaitTermination(timeoutInMinutesForLoading, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String msg = "Necromancer is taking too long to load the maps. Gave up.";
            throw new FastObjectLoaderException(msg);
        }

        if (lastReadRequest == null) {
            log.info("killNecromancer: no read requests have been processed.");
            return;
        }

        // Only waiting on the last requests seems like a hack, but it should
        // be correct, because the fast loader has the following invariants:
        // 1. Will submit read requests in order for the whole range [trimMark, tail]
        // 2. Will verify that all entries are processed in strictly ascending order
        // 3. Kill killNecromancer will only be invoked after all the requests have
        //    been submitted
        // Because of #1, #2 and #3 , if awaitTermination doesn't fail, then lastReadRequest
        // will be the last request submitted
        CFUtils.getUninterruptibly(lastReadRequest);
    }

    /**
     * These two functions are called if no parameter were supplied
     * by the user.
     */
    private void findAndSetLogHead() {
         logHead = 0L;
    }

    private void findAndSetLogTail() {
        logTail = runtime.getAddressSpaceView().getLogTail();
    }

    private void resetAddressProcessed() {
        addressProcessed = logHead - 1;
    }

    /**
     * Check if this entry is relevant
     *
     * There are 2 cases where an entry should not be processed:
     *   1. In whitelist mode, if the stream is not in the whitelist (streamToLoad)
     *   2. In blacklist mode, if the stream is in the blacklist (streamToIgnore)
     *
     * @param streamId identifies the Corfu stream
     * @return if we need to apply the entry.
     */
    private boolean shouldEntryBeApplied(UUID streamId) {
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
        for (UUID id : logData.getStreams()) {
            if (shouldStreamBeProcessed(id)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update the corfu object and it's underlying stream with the new entry.
     *
     * @param logEntry          logEntry from which to apply update.
     * @param globalAddress     global address of the entry.
     */
    private void updateCorfuObjectWithSMRLogEntry(LogEntry logEntry, long globalAddress) {
        SMRLogEntry multiObjectLogEntry = (SMRLogEntry) logEntry;
        multiObjectLogEntry.getEntryMap().forEach((streamId, smrRecords) -> {
            if (shouldEntryBeApplied(streamId) && !smrRecords.isEmpty()) {

                // Get the serializer type from the entry
                ISerializer serializer = Serializers.getSerializer(smrRecords.get(0).getSerializerType().getType());

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

                ISMRStream.addLocatorToSMRRecords(smrRecords, logEntry.getGlobalAddress(), streamId);

                CorfuCompileProxy cp = getCorfuCompileProxy(runtime, streamId, objectType);
                cp.getUnderlyingObject().applyUpdatesToStreamUnsafe(smrRecords, globalAddress);
            }
        });
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
            case SMRLOG:
                updateCorfuObjectWithSMRLogEntry(logEntry, globalAddress);
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
        if (logHead == Address.NON_EXIST) {
            findAndSetLogHead();
        }

        if (logTail == Address.NON_EXIST) {
            findAndSetLogTail();
        }

        resetAddressProcessed();
    }

    /**
     * Clean up all client caches and reset counters then continue loading process from the trim mark
     */
    private void cleanUpForRetry() {
        runtime.getAddressSpaceView().invalidateClientCache();
        runtime.getObjectsView().getObjectCache().clear();

        // Re ask for the Head, if it changes while we were trying.
        findAndSetLogHead();

        nextRead = logHead;
        resetAddressProcessed();
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

        cleanUpForRetry();
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
                if (shouldLogDataBeProcessed(logData)) {
                    updateCorfuObject(logData);
                }
                break;
            case HOLE:
                break;
            case EMPTY:
                log.warn("applyForEachAddress[address={}] is empty");
                break;
            case RANK_ONLY:
                break;
            case GARBAGE:
                break;
            case COMPACTED:
                break;
            default:
                break;
        }
    }

    /**
     * This method will use the checkpoints and the entries
     * after checkpoints to resurrect the SMRMaps
     */
    private void recoverRuntime() {
        log.info("recoverRuntime: Resurrecting the runtime");
        applyForEachAddress(this::processLogData);
    }

    /**
     * Entry point to load the SMRMaps in memory.
     *
     * When this function returns, the maps are fully loaded.
     */
    public void loadMaps() {
        log.info("loadMaps: Starting to resurrect maps");
        initializeHeadAndTails();
        recoverRuntime();

        log.info("loadMaps[startAddress: {}, stopAddress (included): {}, addressProcessed: {}]",
                logHead, logTail, addressProcessed);
        log.info("loadMaps: Loading successful, Corfu maps are alive!");
    }


    /**
     * This method will apply for each address the consumer given in parameter.
     * The Necromancer thread is used to do the heavy lifting.
     */
    private void applyForEachAddress(BiConsumer<Long, ILogData> logDataProcessor) {

        summonNecromancer();
        nextRead = logHead;
        while (nextRead <= logTail) {
            try {
                final long lower = nextRead;
                final long upper = Math.min(lower + batchReadSize - 1, logTail);
                nextRead = upper + 1;

                // Don't cache the read results on server for fast loader
                ContiguousSet<Long> addresses = ContiguousSet.create(
                        Range.closed(lower, upper), DiscreteDomain.longs());

                Map<Long, ILogData> range = runtime.getAddressSpaceView().read(addresses,
                        RecoveryUtils.fastLoaderReadOptions);

                invokeNecromancer(range, logDataProcessor);
            } catch (Exception ex) {
                log.warn("Error loading data", ex);
                handleRetry();
            }
        }
        killNecromancer();
    }

    /**
     * This queue implementation is to be used by single threaded exeuctors
     * to restrict the amount of pending job submissions.
     */
    public class BoundedQueue<E> extends ArrayBlockingQueue<E> {

        public BoundedQueue(int size) {
            // This queue will be used to processes a consecutive range
            // of elements: FIFO order is needed. Thus, fair=true
            super(size, true);
        }

        @Override
        public boolean offer(E e) {
            try {
                put(e);
                return true;
            } catch(InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            // Needed to cause the consumer executor to throw a RejectedExecutionException
            return false;
        }
    }
}

