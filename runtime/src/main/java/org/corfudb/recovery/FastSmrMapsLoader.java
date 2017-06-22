package org.corfudb.recovery;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectsView;

/** The FastSmrMapsLoader reconstructs the coalesced state of SMRMaps through sequential log read
 *
 * This utility reads Log entries sequentially extracting the SMRUpdates from each entry
 * and build the Maps as we go. In the presence of checkpoints, the checkpoint entries will
 * be applied before the normal entries starting after the checkpoint start address.
 *
 * As current state, it doesn't support checkpoints/trim.
 *
 *
 * Created by rmichoud on 6/14/17.
 */

@Slf4j
@Accessors(chain = true)
public class FastSmrMapsLoader {

    static final long DEFAULT_BATCH_FOR_FAST_LOADER = 5;
    static final int DEFAULT_TIMEOUT_MINUTES_FAST_LOADING = 30;
    static final int STATUS_UPDATE_PACE = 10000;

    private CorfuRuntime runtime;

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

    private long addressProcessed = -1;

    // In charge of summoning Corfu maps back in this world
    private ExecutorService necromancer;

    private Map<UUID, StreamMetaData> streamsMetaData;

    @Getter
    private Map<UUID, Long> streamTails = new HashMap<>();

    public FastSmrMapsLoader(@Nonnull final CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        loadInCache = !corfuRuntime.cacheDisabled;
        streamsMetaData = new HashMap<>();
        necromancer = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("necromancer-%d").build());
    }

    /** Check if this entry is relevant
     *
     * Entries before checkpoint starts are irrelevant since they are
     * contained in the checkpoint.
     *
     * @param streamId identifies the Corfu stream
     * @param entry entry to potentially apply
     * @return if we need to apply the entry.
     */
    private boolean shouldEntryBeApplied(UUID streamId, SMREntry entry) {
        if (!streamsMetaData.containsKey(streamId)) {
            return true;
        }

        return (entry.getEntry().getGlobalAddress() >=
                streamsMetaData.get(streamId).getHeadAddress());
    }

    private ObjectsView.ObjectID getObjectIdFromStreamID(UUID streamId) {
        return new ObjectsView.ObjectID(streamId, SMRMap.class);
    }

    /** Update the corfu object and it's underlying stream with the new entry.
     *
     * @param streamId identifies the Corfu stream
     * @param entry entry to apply
     */
    private void applySmrEntryToStream(UUID streamId, SMREntry entry) {
        if (shouldEntryBeApplied(streamId, entry)) {
            createSmrMapIfNotExist(streamId);

            ObjectsView.ObjectID thisObjectId = new ObjectsView.ObjectID(streamId, SMRMap.class);
            CorfuCompileProxy cp = ((CorfuCompileProxy) ((ICorfuSMR) runtime.getObjectsView().getObjectCache().get(thisObjectId)).
                    getCorfuSMRProxy());

            cp.getUnderlyingObject().applyUpdateToStreamUnsafe(entry);
        }
    }

    /** Fetch LogData from Corfu server
     *
     * @param address address to be fetched
     * @return LogData at address
     */
    private ILogData getLogData(long address) {
        if (loadInCache) {
            return runtime.getAddressSpaceView().read(address);
        } else {
            return runtime.getAddressSpaceView().fetch(address);
        }
    }

    /** Get a range of LogData from the server
     *
     * This is using the underlying bulk read implementation for
     * fetching a range of addresses. This read will return
     * a map ordered by address.
     *
     * It uses a ClosedOpen range : [start, end)
     * (e.g [0, 5) == (0,1,2,3,4))
     *
     * @param start start address for the bulk read
     * @param end end address for the bulk read
     * @return logData map ordered by addresses (increasing)
     */
    private Map<Long, ILogData> getLogData(long start, long end) {
        return runtime.getAddressSpaceView()
                .cacheFetch(ContiguousSet.create(Range.closedOpen(start, end), DiscreteDomain.longs()));
    }


    /** Create a new object SMRMap as recipient of SMRUpdates (if doesn't exist yet)
     *
     * @param streamId
     */
    private void createSmrMapIfNotExist(UUID streamId) {
        if (!runtime.getObjectsView().getObjectCache().containsKey(getObjectIdFromStreamID(streamId))) {
            runtime.getObjectsView().build()
                    .setStreamID(streamId)
                    .setType(SMRMap.class)
                    .open();
        }
    }

    /** Deserialize a logData by getting the logEntry
     *
     * Getting the underlying logEntry should trigger deserialization only once.
     * Next access should just returned the logEntry direclty.
     *
     * @param logData
     * @return
     * @throws Exception
     */
    public LogEntry deserializeLogData(ILogData logData) throws Exception{
        return logData.getLogEntry(runtime);
    }

    /** Extract log entries from logData and update the Corfu Objects
     *
     * @param logData LogData received from Corfu server.
     */
    private void processLogData(ILogData logData) {

        LogEntry logEntry;
        try {
            logEntry = deserializeLogData(logData);
        } catch (Exception e) {
            log.error("Cannot deserialize log entry" + logData.getGlobalAddress(), e);
            return;
        }

        if (logEntry.getType() == LogEntry.LogEntryType.SMR) {
            // Just one stream, always
            UUID streamId = logData.getStreams().iterator().next();
            applySmrEntryToStream(streamId, (SMREntry) logEntry);
        }
        else if (logEntry.getType() == LogEntry.LogEntryType.MULTIOBJSMR) {
            MultiObjectSMREntry multiObjectLogEntry = (MultiObjectSMREntry) logEntry;
            multiObjectLogEntry.getEntryMap().forEach((stream, multiSmrEntry) -> {
                multiSmrEntry.getSMRUpdates(stream).forEach((smrEntry) -> {
                    applySmrEntryToStream(stream, smrEntry);
                });
            });
        }
    }

    private void findAndSetLogHead() {
        logHead = 0;
    }

    private void findAndSetLogTail() {
        logTail = runtime.getSequencerView().nextToken(Collections.emptySet(), 0).getTokenValue();
    }

    /** Initialize log head and log tails
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
    }

    /** Book keeping of the the stream tails
     *
     * @param logData
     */
    public void updateStreamTails(ILogData logData) {
         for (UUID streamId : logData.getStreams()) {
             streamTails.put(streamId, logData.getGlobalAddress());
            }
    }

    /** Processing logdata at a given log address
     *
     * @param logData
     * @param address
     */
    public void processAddressLogData(ILogData logData, long address) {
        switch (logData.getType()) {
            case DATA:
                processLogData(logData);
                break;
            case HOLE:
                break;
            case TRIMMED:
                log.warn("loadMaps[{}, start={}]: address is trimmed", address, logHead);
                // Should not happen
            case EMPTY:
                log.warn("loadMaps[address={}]: is empty", address);
                break;
            case RANK_ONLY:
                break;
            default:
                break;
        }
    }


    /** Entry point to load the SMRMaps in memory.
     *
     * When this function returns, the maps are fully loaded.
     *
     */
    public void loadMaps() {
        log.info("loadMaps: Starting to resurrect maps");
        initializeHeadAndTails();

        long nextRead = 0;
        while (nextRead <= logTail) {
            final long start = nextRead;
            final long stopNotIncluded = Math.min(start + batchReadSize, logTail + 1);
            nextRead = stopNotIncluded;
            final Map<Long, ILogData> range = getLogData(start, stopNotIncluded);
            for (Long address : range.keySet()) {
                if (address != addressProcessed + 1) {
                    throw new RuntimeException("loadMaps: Missed an entry, this implies correctness issues");
                }
                addressProcessed++;
                if (addressProcessed % STATUS_UPDATE_PACE == 0) {
                    log.info("LoadMaps: read up to {}", address);
                }
            }

            necromancer.execute(() -> {
                range.forEach((address, logData) -> {
                    if (recoverSequencerMode) {
                        updateStreamTails(logData);
                    } else {
                        processAddressLogData(logData, address);
                    }
                });
            });
        }

        necromancer.shutdown();

        try {
            necromancer.awaitTermination(timeoutInMinutesForLoading, TimeUnit.MINUTES);
            log.info("loadMaps[startAddress: {}, stopAddress (included): {}, addressProcessed: {}]",
                    logHead, logTail, addressProcessed);
            log.info("loadMaps: Loading successful, Corfu maps are alive!");
        } catch (InterruptedException e) {
            log.warn("loadMaps: Taking too long to load the maps. Gave up.");
        }
    }

    @Data
    private class StreamMetaData {
        UUID streamId;
        long headAddress;
    }
}

