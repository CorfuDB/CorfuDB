package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.infrastructure.logreplication.utils.SnapshotSyncUtils;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;
import org.corfudb.runtime.exceptions.StreamingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig.CLIENT_CONFIG_TAG;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


/**
 * This class implements a Corfu stream listener for the Logical Group client configuration tables.
 * It is used by LR to listen to client registration events and logical group destination information updates.
 */
@Slf4j
public class LogReplicationClientConfigListener extends StreamListenerResumeOrFullSync {

    /**
     * Tables that this stream listener will be listening to.
     */
    private static final List<String> tablesOfInterest = new ArrayList<>(
            Arrays.asList(LR_REGISTRATION_TABLE_NAME, LR_MODEL_METADATA_TABLE_NAME));

    /**
     * Accessing SessionManager for client register and unregister, which could lead to session creation and termination.
     */
    private final SessionManager sessionManager;

    /**
     * Accessing LogReplicationConfigManager to react accordingly when new entries found in client config tables
     */
    private final LogReplicationConfigManager configManager;

    /**
     * CorfuStore for subscribing this StreamListener
     */
    private final CorfuStore corfuStore;

    /**
     * Flag to indicate whether this stream listener has been started.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public LogReplicationClientConfigListener(SessionManager sessionManager,
                                              LogReplicationConfigManager configManager,
                                              CorfuStore corfuStore) {
        super(corfuStore, CORFU_SYSTEM_NAMESPACE, CLIENT_CONFIG_TAG, tablesOfInterest);
        this.sessionManager = sessionManager;
        this.configManager = configManager;
        this.corfuStore = corfuStore;
    }

    /**
     * Subscribe this stream listener to start monitoring the changes of LR client config tables.
     */
    public void start() {
        CorfuStoreMetadata.Timestamp timestamp = configManager.preprocessAndGetTail();
        configManager.generateConfig(sessionManager.getSessions());

        log.info("Start log replication listener for client config tables from {}", timestamp);
        try {
            corfuStore.subscribeListener(this, CORFU_SYSTEM_NAMESPACE, CLIENT_CONFIG_TAG, tablesOfInterest, timestamp);
            started.set(true);
        } catch (StreamingException e) {
            if (e.getExceptionCause().equals(StreamingException.ExceptionCause.LISTENER_SUBSCRIBED)) {
                log.error("Stream listener already registered!");
            } else {
                log.error("Stream listener subscribe failed!", e);
                throw e;
            }
        }
    }

    /**
     * A corfu update can/may have multiple updates belonging to different streams.
     * This callback will return those updates as a list grouped by their Stream UUIDs.
     * <p>
     * Note: there is no order guarantee within the transaction boundaries.
     *
     * @param results is a map of stream UUID -> list of entries of this stream.
     */
    @Override
    public void onNext(CorfuStreamEntries results) {
        log.info("Client config listener onNext: {}", results.getEntries().size());
        results.getEntries().forEach((key, value) -> {
            log.info("Key table name: {}", key.getTableName());
            String tableName = key.getTableName();
            switch (tableName) {
                case LR_REGISTRATION_TABLE_NAME:
                    handleRegistrationTableEntries(value);
                    break;
                case LR_MODEL_METADATA_TABLE_NAME:
                    handleClientMetadataTableEntries(value);
                    break;
                default:
                    break;
            }
        });
    }

    /**
     * Handle client registration table entries.
     * @param registrationTableEntries list of client registration table entries
     */
    private void handleRegistrationTableEntries(List<CorfuStreamEntry> registrationTableEntries) {
        if (registrationTableEntries == null) {
            log.warn("No client registration table entries found!");
            return;
        }
        for (CorfuStreamEntry entry : registrationTableEntries) {
            if (entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                String clientName = ((ClientRegistrationId) entry.getKey()).getClientName();
                ReplicationModel model = ((ClientRegistrationInfo) entry.getPayload()).getModel();
                LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                        .setClientName(clientName).setModel(model).build();
                if (model.equals(ReplicationModel.LOGICAL_GROUPS) || model.equals(ReplicationModel.ROUTING_QUEUES)) {
                    if(sessionManager.getReplicationContext().getIsLeader().get()) {
                        configManager.onNewClientRegister(subscriber);
                        Set<LogReplicationSession> sessionForSinkSide = sessionManager.createOutgoingSessionsBySubscriber(subscriber);
                        sessionManager.sendOutSessionToSinkSide(sessionForSinkSide);
                    }
                    if (sessionManager.isConnectionReceiver()) {

                    }
                }
                log.info("New client {} registered with model {}", clientName, model);
            } else if (entry.getOperation().equals(CorfuStreamEntry.OperationType.DELETE)) {
                // TODO (V2 / Chris/Shreay): add unregister API for clients
                String clientName = ((ClientRegistrationId) entry.getKey()).getClientName();
                log.info("Client {} unregistered", clientName);
            }
        }
    }

    /**
     * Handle client metadata table entries.
     * @param clientMetadataTableEntries list of client metadata table entries
     */
    private void handleClientMetadataTableEntries(List<CorfuStreamEntry> clientMetadataTableEntries) {
        Set<LogReplicationSession> impactedSessions = new HashSet<>();
        log.info("clientMetadataTableEntries: {}", clientMetadataTableEntries);
        if (clientMetadataTableEntries == null) {
            log.warn("No client metadata table entries found!");
            return;
        }

        for (CorfuStreamEntry entry : clientMetadataTableEntries) {
            ClientDestinationInfoKey clientInfo = (ClientDestinationInfoKey) entry.getKey();
            LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                    .setClientName(clientInfo.getClientName()).setModel(clientInfo.getModel()).build();
            if (entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                DestinationInfoVal sinksInfo = (DestinationInfoVal) entry.getPayload();
                impactedSessions = configManager.onGroupDestinationsChange(subscriber, clientInfo.getGroupName(),
                        sinksInfo.getDestinationIdsList());
            } else if (entry.getOperation().equals(CorfuStreamEntry.OperationType.DELETE)) {
                impactedSessions = configManager.onGroupDestinationsChange(subscriber, clientInfo.getGroupName(),
                        new ArrayList<>());
            }

            if (impactedSessions != null) {
                log.info("Sessions that a forced snapshot sync will be triggered: {}", impactedSessions);
                impactedSessions.forEach(session -> {
                    SnapshotSyncUtils.enforceSnapshotSync(session, corfuStore);
                });
            }
        }
    }

    /**
     * Unsubscribe this stream listener to stop
     */
    public void stop() {
        corfuStore.unsubscribeListener(this);
        started.set(false);
    }

    /**
     * Check if the listener is started.
     * @return true if started, false otherwise.
     */
    public boolean listenerStarted() {
        return started.get();
    }

    /**
     * Perform full sync (snapshot) on a table or tables of interest (the ones subscribed to).
     *
     * @return timestamp from which to subscribe for deltas.
     * Note: this timestamp should come from txn.commit() of the full sync.
     */
    @Override
    protected CorfuStoreMetadata.Timestamp performFullSync() {
        // TODO (V2 / Chris): In next PR this listener will be only for client register/unregister, remember to avoid
        //  clearing all the in-memory fields in onClientListenerResume method in next PR.
        CorfuStoreMetadata.Timestamp timestamp = configManager.onClientListenerResume();
        configManager.generateConfig(sessionManager.getSessions());
        sessionManager.getSessions().forEach(session -> {
            SnapshotSyncUtils.enforceSnapshotSync(session, corfuStore);
        });
        return timestamp;
    }
}
