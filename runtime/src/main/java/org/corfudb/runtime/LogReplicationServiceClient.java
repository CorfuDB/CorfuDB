package org.corfudb.runtime;

import com.google.common.base.Strings;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * A client for log replication
 */
@Slf4j
public class LogReplicationServiceClient {

    public static final String LR_REGISTRY_TABLE_NAME = "LogReplicationRegistrationTable";
    public static final String LR_SOURCE_METADATA_TABLE_NAME = "LogReplicationSourceMetadataTable";

    /**
     * Wait interval between consecutive sync attempts to cap exponential back-off
     */
    private static final int SYNC_THRESHOLD = 120;

    private final CorfuStore corfuStore;
    private final Table<ClientRegistrationId, ClientRegistrationInfo, Message> replicationRegistrationTable;
    private final Table<ClientDestinationInfoKey, DestinationInfoVal, Message> sourceMetadataTable;

    private final ClientRegistrationId clientKey;
    private final ClientRegistrationInfo clientInfo;

    private final String clientName;
    private final String modelEnum;

    /**
     * Constructor for LogReplicationClient
     *
     * @param runtime Corfu Runtime
     * @param clientName String representation of the client name
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws IllegalAccessException IllegalAccessException
     */
    public LogReplicationServiceClient(CorfuRuntime runtime, String clientName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        this.corfuStore = new CorfuStore(runtime);
        this.clientName = clientName;

        this.modelEnum = "LOGICAL_GROUPS";

        this.clientKey = ClientRegistrationId.newBuilder()
                .setClientName(clientName)
                .build();
        this.clientInfo = ClientRegistrationInfo.newBuilder()
                .setClientName(clientName)
                .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                .setRegistrationTime(Timestamp.newBuilder().setSeconds(new Date().getTime() / 1000).build())
                .build();

        this.replicationRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRY_TABLE_NAME, ClientRegistrationId.class,
                ClientRegistrationInfo.class, null, TableOptions.builder().build());
        this.sourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME, ClientDestinationInfoKey.class,
                DestinationInfoVal.class, null, TableOptions.builder().build());

        register();
    }

    /**
     * Adds client and model to LogReplicationRegistrationTable
     */
    private void register() {
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    if (txn.isExists(LR_REGISTRY_TABLE_NAME, clientKey)) {
                        log.warn("Client already registered \nclientKey: {} \nclientInfo: {}",
                                clientKey, txn.getRecord(replicationRegistrationTable, clientKey).getPayload().toString());
                    }
                    else {
                        txn.putRecord(replicationRegistrationTable, clientKey, clientInfo, null);
                        txn.commit();
                    }
                }
                catch (Exception e) {
                    log.error("Unable to register client {}, Error: {}", clientName, e);
                }
                return null;
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add destination to LogReplicationSourceMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param destination Cluster which the logicalGroup should be replicated to
     * @return true if destination was newly registered
     */
    public boolean addDestination(String logicalGroup, String destination) {
        if (Strings.isNullOrEmpty(logicalGroup) || Strings.isNullOrEmpty(destination)) {
            log.error("Invalid logicalGroup or destination");
            return false;
        }
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    DestinationInfoVal clientDestinations;

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        if (clientDestinations.getDestinationIdsList().contains(destination)) {
                            log.warn("Destination already added");
                            return false;
                        }

                        clientDestinations = clientDestinations.toBuilder().addDestinationIds(destination).build();
                    }
                    else {
                        clientDestinations = DestinationInfoVal.newBuilder().addDestinationIds(destination).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();

                    return true;
                }
                catch (Exception e) {
                    log.error("Unable to add destination", e);
                    return false;
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add a list destination to LogReplicationSourceMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param remoteDestinations Clusters which the logicalGroup should be replicated to
     * @return true if all destinations were newly registered
     */
    public boolean addDestination(String logicalGroup, List<String> remoteDestinations) {
        if (Strings.isNullOrEmpty(logicalGroup) || remoteDestinations == null || remoteDestinations.isEmpty()) {
            log.error("Invalid logicalGroup or destination(s)");
            return false;
        }
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    DestinationInfoVal clientDestinations;

                    boolean hasNullOrEmptyOrDuplicates =
                            remoteDestinations.stream().anyMatch(Objects::isNull) ||
                            remoteDestinations.stream().anyMatch(String::isEmpty) ||
                            remoteDestinations.stream().distinct().count() < remoteDestinations.size();

                    if (hasNullOrEmptyOrDuplicates) {
                        log.error("Invalid destination(s) found in list");
                        return false;
                    }

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        List<String> newDestinations = new ArrayList<>(remoteDestinations);
                        newDestinations.removeAll(clientDestinations.getDestinationIdsList());

                        if (newDestinations.isEmpty()) {
                            log.debug("All destinations already present");
                            return true;
                        }

                        clientDestinations = clientDestinations.toBuilder().addAllDestinationIds(newDestinations).build();
                    }
                    else {
                        clientDestinations = DestinationInfoVal.newBuilder().addAllDestinationIds(remoteDestinations).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();

                    return true;
                }
                catch (Exception e) {
                    log.error("Unable to add destinations", e);
                    return false;
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove destination from LogReplicationSourceMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param destination Cluster from which the logicalGroup should be removed from further replication
     * @return true if destination was removed
     */
    public boolean removeDestination(String logicalGroup, String destination) {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        if (clientDestinationIdsList.remove(destination)) {
                            clientDestinationIds = clientDestinationIds.toBuilder()
                                    .clearDestinationIds()
                                    .addAllDestinationIds(clientDestinationIdsList)
                                    .build();
                            txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinationIds, null);
                            txn.commit();

                            return true;
                        }
                        log.debug("Destination not found");
                        return false;
                    }
                    throw new Exception(String.format("Record not found for group: %s", logicalGroup));
                }
                catch (Exception e) {
                    log.error("Unable to remove destination", e);
                    return false;
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove a list of destinations from LogReplicationSourceMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param remoteDestinations Cluster from which the logicalGroup should be removed from further replication
     * @return true if destinations were removed
     */
    public boolean removeDestination(String logicalGroup, List<String> remoteDestinations) {
        if (Strings.isNullOrEmpty(logicalGroup) || remoteDestinations == null || remoteDestinations.isEmpty()) {
            log.error("Invalid logicalGroup or destination(s)");
            return false;
        }
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    boolean hasNullOrEmptyOrDuplicates =
                            remoteDestinations.stream().anyMatch(Objects::isNull) ||
                            remoteDestinations.stream().anyMatch(String::isEmpty) ||
                            remoteDestinations.stream().distinct().count() < remoteDestinations.size();

                    if (hasNullOrEmptyOrDuplicates) {
                        log.error("Invalid destination(s) found in list");
                        return false;
                    }

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        int initialNumDestinations = clientDestinationIdsList.size();
                        clientDestinationIdsList.removeAll(remoteDestinations);

                        if (clientDestinationIdsList.size() == initialNumDestinations) {
                            log.debug("No matching destinations found");
                            return false;
                        }
                        else if (clientDestinationIdsList.size() + remoteDestinations.size() > initialNumDestinations) {
                            log.warn("Not all destinations to remove are present");
                        }

                        clientDestinationIds = clientDestinationIds.toBuilder()
                                .clearDestinationIds()
                                .addAllDestinationIds(clientDestinationIdsList)
                                .build();
                        txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinationIds, null);
                        txn.commit();

                        return true;
                    }
                    throw new Exception(String.format("Record not found for group: %s", logicalGroup));
                }
                catch (Exception e) {
                    log.error("Unable to remove destinations", e);
                    return false;
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }
}
