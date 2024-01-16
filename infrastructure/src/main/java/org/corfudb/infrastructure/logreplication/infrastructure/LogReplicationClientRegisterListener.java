package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.infrastructure.logreplication.utils.SnapshotSyncUtils;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;
import org.corfudb.runtime.exceptions.StreamingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig.CLIENT_CONFIG_TAG;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


/**
 * This class implements a Corfu stream listener for the Logical Group client registration table.
 * It is used by LR to listen to client registration events and create outgoing sessions from Source side
 */
@Slf4j
public class LogReplicationClientRegisterListener extends StreamListenerResumeOrFullSync {

    /**
     * This listener will be listening to LogReplicationRegistrationTable.
     */
    private static final List<String> tablesOfInterest = new ArrayList<>(
            Collections.singletonList(LR_REGISTRATION_TABLE_NAME));

    /**
     * Accessing SessionManager for client register and unregister, which could lead to session creation and termination.
     */
    private final SessionManager sessionManager;

    /**
     * Accessing LogReplicationConfigManager to generating config for created session upon listener start and resume.
     */
    private final LogReplicationConfigManager configManager;

    /**
     * CorfuStore for subscribing this StreamListener
     */
    private final CorfuStore corfuStore;

    /**
     * Flag to indicate whether this stream listener has been started or not.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public LogReplicationClientRegisterListener(SessionManager sessionManager,
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
        Pair<Set<LogReplication.ReplicationSubscriber>, CorfuStoreMetadata.Timestamp> subscribersAndLogTail = configManager.preprocessAndGetTail();
        subscribersAndLogTail.getLeft().forEach(sessionManager :: createSessions);
        CorfuStoreMetadata.Timestamp tail = subscribersAndLogTail.getRight();

        log.info("Start log replication listener for client registration table from {}", tail);
        try {
            corfuStore.subscribeListener(this, CORFU_SYSTEM_NAMESPACE, CLIENT_CONFIG_TAG, tablesOfInterest, tail);
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
        results.getEntries().forEach((key, value) -> {
            String tableName = key.getTableName();
            if (tableName.equals(LR_REGISTRATION_TABLE_NAME)) {
                handleRegistrationTableEntries(value);
            } else {
                log.warn("Client registration listener receives entries from unexpected table: {}", tableName);
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
            String clientName = ((ClientRegistrationId) entry.getKey()).getClientName();
            ReplicationModel model = ((ClientRegistrationInfo) entry.getPayload()).getModel();
            LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                    .setClientName(clientName).setModel(model).build();

            if (entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                configManager.onNewClientRegister(subscriber);
                sessionManager.createSessions(subscriber);
                log.info("New client {} registered with model {}", clientName, model);
            } else if (entry.getOperation().equals(CorfuStreamEntry.OperationType.DELETE)) {
                // TODO (V2 / Chris/Shreay): add unregister API for clients.
                //  Currently the in-memory subscriber info will be removed so that in the event of of a switchover
                //  the old active (new standby) doesn't have the subscriber information
                configManager.getRegisteredSubscribers().remove(subscriber);
                log.info("Client {} unregistered", clientName);
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
        Pair<Set<LogReplication.ReplicationSubscriber>, CorfuStoreMetadata.Timestamp> subscribersAndTs = configManager.onClientListenerResume();
        subscribersAndTs.getLeft().forEach(sessionManager::createSessions);
        sessionManager.getOutgoingSessions().forEach(session -> {
            SnapshotSyncUtils.enforceSnapshotSync(session, corfuStore,
                    LogReplication.ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC);
        });
        return subscribersAndTs.getRight();
    }
}
