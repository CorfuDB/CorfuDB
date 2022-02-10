package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.REGISTRY_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private final LogReplicationMetadataManager logReplicationMetadataManager;
    private final LogReplicationConfig config;
    private final Map<UUID, List<SMREntry>> pendingEntries;
    private final CorfuRuntime rt;
    // Set of streams newly opened and replicated from ACTIVE, need to clear local
    // writes to avoid polluting those streams.
    private final Set<UUID> streamsToClear;
    private Set<UUID> streamsSet; //the set of streams that log entry writer will work on.
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.
    private Map<UUID, Set<UUID>> dataStreamToTagsMap;
    private boolean handlePendingEntries;

    public LogEntryWriter(LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager,
                          CorfuRuntime rt) {
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.config = config;
        this.streamsToClear = new HashSet<>();
        this.srcGlobalSnapshot = Address.NON_ADDRESS;
        this.lastMsgTs = Address.NON_ADDRESS;
        this.handlePendingEntries = false;
        this.pendingEntries = new HashMap<>();
        this.rt = rt;
    }

    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) throws ReplicationWriterException {
        if (metadata.getEntryType() != LogReplicationEntryType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting type {} snapshot {}",
                    TextFormat.shortDebugString(metadata),
                    LogReplicationEntryType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     * @return true when the msg is appended to the log
     */
    private boolean processMsg(LogReplicationEntryMsg txMessage) {
        List<OpaqueEntry> opaqueEntryList = extractOpaqueEntries(txMessage);
        UUID registryTableId = CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                REGISTRY_TABLE_NAME));

        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {

            Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                    LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
            long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedSnapshotDone = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
            long persistedLogTs = metadataMap.get(LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);

            long topologyConfigId = txMessage.getMetadata().getTopologyConfigID();
            long baseSnapshotTs = txMessage.getMetadata().getSnapshotTimestamp();
            long entryTs = txMessage.getMetadata().getTimestamp();
            long prevTs = txMessage.getMetadata().getPreviousTimestamp();

            lastMsgTs = Math.max(persistedLogTs, lastMsgTs);

            if (topologyConfigId != persistedTopologyConfigId || baseSnapshotTs != persistedSnapshotStart ||
                    baseSnapshotTs != persistedSnapshotDone || prevTs != persistedLogTs) {
                log.warn("Message metadata mismatch. Skip applying message {}, persistedTopologyConfigId={}, persistedSnapshotStart={}, " +
                                "persistedSnapshotDone={}, persistedLogTs={}", txMessage.getMetadata(), persistedTopologyConfigId,
                        persistedSnapshotStart, persistedSnapshotDone, persistedLogTs);
                return false;
            }

            // Skip Opaque entries with timestamp that are not larger than persistedTs
            OpaqueEntry[] newOpaqueEntryList = opaqueEntryList.stream().filter(x -> x.getVersion() > persistedLogTs).toArray(OpaqueEntry[]::new);

            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, entryTs);

            for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {

                // Sync with state in LogReplicationConfig after each update
                this.streamsSet = new HashSet<>(config.getStreamsInfo().getStreamIds());
                this.dataStreamToTagsMap = new HashMap<>(config.getDataStreamToTagsMap());
                for (UUID streamId : opaqueEntry.getEntries().keySet()) {
                    if (streamId.equals(registryTableId)) {
                        handlePendingEntries = true;
                    }

                    if (!streamsSet.contains(streamId)) {
                        log.debug("Stream with id {} created during log entry sync and arrived before Registry Table," +
                                "temporarily added to pending entries", streamId);
                        // At this point we know that all the entries are valid to apply. If we cannot get stream tags from
                        // the map, it means this is a newly opened stream during delta sync and the registry table on standby
                        // side don't have records. We mark them as pending entries and apply after registry table is updated.
                        pendingEntries.putIfAbsent(streamId, new ArrayList<>());
                        pendingEntries.get(streamId).addAll(opaqueEntry.getEntries().get(streamId));
                        streamsToClear.add(streamId);
                        continue;
                    }

                    for (SMREntry smrEntry : opaqueEntry.getEntries().get(streamId)) {
                        // If stream tags exist for the current stream, it means it's intended for streaming on the Sink (receiver)
                        List<UUID> streamTags = new ArrayList<>();
                        if (dataStreamToTagsMap.containsKey(streamId)) {
                            streamTags.addAll(dataStreamToTagsMap.get(streamId));
                        }
                        txnContext.logUpdate(streamId, smrEntry, streamTags);
                    }
                }
            }
            txnContext.commit();
            lastMsgTs = Math.max(entryTs, lastMsgTs);
        }

        if (handlePendingEntries) {
            updateLogReplicationConfig();
            this.streamsSet = new HashSet<>(config.getStreamsInfo().getStreamIds());
            this.dataStreamToTagsMap = new HashMap<>(config.getDataStreamToTagsMap());
            clearNewStreamsWithLocalWrites();
            if (!pendingEntries.isEmpty()) {
                try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                    pendingEntries.entrySet().removeIf(mapEntry -> {
                        UUID streamId = mapEntry.getKey();
                        boolean toRemove = streamsSet.contains(streamId);
                        if (toRemove) {
                            log.info("Handle pending entry for stream id: {}", streamId);
                            for (SMREntry smrEntry : pendingEntries.get(streamId)) {
                                List<UUID> streamTags = new ArrayList<>();
                                if (dataStreamToTagsMap.containsKey(streamId)) {
                                    streamTags.addAll(dataStreamToTagsMap.get(streamId));
                                }
                                txnContext.logUpdate(streamId, smrEntry, streamTags);
                            }
                        }
                        return toRemove;
                    });
                    txnContext.commit();
                }
            }
            handlePendingEntries = false;
        }

        return true;
    }

    private void updateLogReplicationConfig() {
        CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>
                registryTable = rt.getTableRegistry().getRegistryTable();
        Set<UUID> updatedStreamSet = registryTable.entryStream()
                .filter(entry -> entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> CorfuRuntime.getStreamID(getFullyQualifiedTableName(entry.getKey())))
                .collect(Collectors.toSet());
        Map<UUID, Set<UUID>> updatedStreamTagsMap = new HashMap<>();
        registryTable.forEach((tableName, tableRecord) -> {
            UUID streamId = CorfuRuntime.getStreamID(getFullyQualifiedTableName(tableName));
            updatedStreamTagsMap.putIfAbsent(streamId, new HashSet<>());
            updatedStreamTagsMap.get(streamId).addAll(
                    tableRecord.getMetadata()
                            .getTableOptions()
                            .getStreamTagList()
                            .stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(tableName.getNamespace(), streamTag))
                            .collect(Collectors.toList()));
        });
        this.config.updateDataStreamToTagsMap(updatedStreamTagsMap, false);
        this.config.getStreamsInfo().updateStreamIdsOnStandby(updatedStreamSet, false);
    }

    private void clearNewStreamsWithLocalWrites() {
        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
            for (UUID streamId : streamsToClear) {
                long streamTail = rt.getSequencerView().query(streamId);
                if (streamTail != Address.NON_EXIST) {
                    SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
                    List<UUID> streamTags = new ArrayList<>();
                    if (dataStreamToTagsMap.containsKey(streamId)) {
                        streamTags.addAll(dataStreamToTagsMap.get(streamId));
                    }
                    txnContext.logUpdate(streamId, entry, streamTags);
                }
            }
            streamsToClear.clear();
            txnContext.commit();
        }
    }

    /**
     * Apply message at the destination Corfu Cluster
     *
     * @param msg
     * @return true when the msg is appended to the log
     * @throws ReplicationWriterException
     */
    public boolean apply(LogReplicationEntryMsg msg) throws ReplicationWriterException {

        log.debug("Apply log entry {}", msg.getMetadata().getTimestamp());

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Ignore Log Entry. Received message with snapshot {} is smaller than current snapshot {}",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return false;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            log.warn("A new log entry sync is triggered with higher snapshot, previous snapshot " +
                            "is {} and setup the new srcGlobalSnapshot & lastMsgTs as {}",
                    srcGlobalSnapshot, msg.getMetadata().getSnapshotTimestamp());
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            return processMsg(msg);
        }

        log.warn("Log entry {} was not processed, prevTs={}, lastMsgTs={}, srcGlobalSnapshot={}", msg.getMetadata().getTimestamp(),
                msg.getMetadata().getPreviousTimestamp(), lastMsgTs, srcGlobalSnapshot);
        return false;
    }

    /**
     * Set the base snapshot on which the last full sync was based on and ackTimestamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     *
     * @param snapshot the base snapshot on which the last full sync was based on.
     * @param ackTimestamp
     */
    public void reset(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
        handlePendingEntries = false;
        pendingEntries.clear();
        streamsToClear.clear();
    }
}
