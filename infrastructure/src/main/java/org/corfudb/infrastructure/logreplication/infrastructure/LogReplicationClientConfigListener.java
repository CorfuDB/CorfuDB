package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
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
import org.corfudb.runtime.collections.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.CLIENT_CONFIG_TAG;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


@Slf4j
public class LogReplicationClientConfigListener extends StreamListenerResumeOrFullSync {

    /**
     * TableSchema for fetching client registration table entries from CorfuStreamEntries
     */
    private static final TableSchema<ClientRegistrationId, ClientRegistrationInfo, Message> registrationTableSchema =
            new TableSchema<>(LR_REGISTRATION_TABLE_NAME, ClientRegistrationId.class, ClientRegistrationInfo.class, Message.class);

    /**
     * TableSchema for fetching client metadata table entries from CorfuStreamEntries
     */
    private static final TableSchema<ClientDestinationInfoKey, DestinationInfoVal, Message> clientMetadataTableSchema =
            new TableSchema<>(LR_MODEL_METADATA_TABLE_NAME, ClientDestinationInfoKey.class, DestinationInfoVal.class, Message.class);

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

    public LogReplicationClientConfigListener(SessionManager sessionManager, LogReplicationConfigManager configManager, CorfuStore corfuStore) {
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

        log.info("Start log replication listener for client config tables from {}", timestamp);
        corfuStore.subscribeListener(this, CORFU_SYSTEM_NAMESPACE, CLIENT_CONFIG_TAG, tablesOfInterest, timestamp);
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
        List<CorfuStreamEntry> registrationTableEntries = results.getEntries().get(registrationTableSchema);
        for (CorfuStreamEntry entry : registrationTableEntries) {
            if (entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                String clientName = ((ClientRegistrationId) entry.getKey()).getClientName();
                ReplicationModel model = ((ClientRegistrationInfo) entry.getPayload()).getModel();
                LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                        .setClientName(clientName).setModel(model).build();
                configManager.onNewClientRegister(subscriber);
                sessionManager.createOutgoingSessionsBySubscriber(subscriber);
            } else {
                // TODO (V2 / Chris/Shreay): add unregister API for clients
            }
        }

        List<CorfuStreamEntry> clientMetadataTableEntries = results.getEntries().get(clientMetadataTableSchema);
        Set<LogReplicationSession> impactedSessions = new HashSet<>();
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
                impactedSessions.forEach(sessionManager::enforceSnapshotSync);
            }
        }
    }

    /**
     * Unsubscribe this stream listener to stop
     */
    public void stop() {
        corfuStore.unsubscribeListener(this);
    }

    /**
     * Perform full sync (snapshot) on a table or tables of interest (the ones subscribed to).
     *
     * @return timestamp from which to subscribe for deltas.
     * Note: this timestamp should come from txn.commit() of the full sync.
     */
    @Override
    protected CorfuStoreMetadata.Timestamp performFullSync() {
        return configManager.onClientListenerResume();
    }
}
