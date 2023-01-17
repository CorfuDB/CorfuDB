package org.corfudb.runtime;

import com.google.common.base.Preconditions;
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
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * A client for log replication
 */
@Slf4j
public class LogReplicationLogicalGroupClient {

    /**
     * Key: ClientRegistrationId
     * Value: ClientRegistrationInfo
     */
    public static final String LR_REGISTRATION_TABLE_NAME = "LogReplicationRegistrationTable";

    /**
     * Key: ClientDestinationInfoKey
     * Value: DestinationInfoVal
     */
    public static final String LR_MODEL_METADATA_TABLE_NAME = "LogReplicationModelMetadataTable";

    private static final String modelEnum = "LOGICAL_GROUPS";

    private final CorfuStore corfuStore;
    private final Table<ClientRegistrationId, ClientRegistrationInfo, Message> replicationRegistrationTable;
    private final Table<ClientDestinationInfoKey, DestinationInfoVal, Message> sourceMetadataTable;

    private final ClientRegistrationId clientKey;
    private final ClientRegistrationInfo clientInfo;

    private final String clientName;

    /**
     * Constructor for LogReplicationClient
     *
     * @param runtime Corfu Runtime
     * @param clientName String representation of the client name
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws IllegalAccessException IllegalAccessException
     */
    public LogReplicationLogicalGroupClient(CorfuRuntime runtime, String clientName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Table<ClientDestinationInfoKey, DestinationInfoVal, Message> tempSourceMetadataTable;
        Table<ClientRegistrationId, ClientRegistrationInfo, Message> tempReplicationRegistrationTable;
        this.corfuStore = new CorfuStore(runtime);
        this.clientName = clientName;

        this.clientKey = ClientRegistrationId.newBuilder()
                .setClientName(clientName)
                .build();
        Instant time = Instant.now();
        this.clientInfo = ClientRegistrationInfo.newBuilder()
                .setClientName(clientName)
                .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                .setRegistrationTime(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build())
                .build();

        try {
            tempReplicationRegistrationTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME);
        } catch (NoSuchElementException nse) {
            tempReplicationRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME, ClientRegistrationId.class,
                    ClientRegistrationInfo.class, null, TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
        }
        this.replicationRegistrationTable = tempReplicationRegistrationTable;

        try {
            tempSourceMetadataTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME);
        } catch (NoSuchElementException nse) {
            tempSourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME, ClientDestinationInfoKey.class,
                    DestinationInfoVal.class, null, TableOptions.fromProtoSchema(DestinationInfoVal.class));
        }
        this.sourceMetadataTable = tempSourceMetadataTable;

        register();
    }

    /**
     * Adds client and model to LogReplicationRegistrationTable
     */
    private void register() {
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    if (txn.isExists(LR_REGISTRATION_TABLE_NAME, clientKey)) {
                        log.warn("Client already registered.\n--- ClientRegistrationId ---\n{}--- ClientRegistrationInfo ---\n{}",
                                clientKey, txn.getRecord(replicationRegistrationTable, clientKey).getPayload());
                    } else {
                        txn.putRecord(replicationRegistrationTable, clientKey, clientInfo, null);
                        txn.commit();
                    }
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to register client", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Client registration failed", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add destination to LogReplicationModelMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param destination Cluster which the logicalGroup should be replicated to
     */
    public void addDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(destination), String.format("[%s] %s", clientName, "destination is null or empty."));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    DestinationInfoVal clientDestinations;

                    if (txn.isExists(LR_MODEL_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        if (clientDestinations.getDestinationIdsList().contains(destination)) {
                            log.debug("[{}] {}", clientName, "Destination already exists.");
                            return null;
                        }

                        clientDestinations = clientDestinations.toBuilder().addDestinationIds(destination).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addDestinationIds(destination).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to add destination, will be retried", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Unable to add destination", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add a list destination to LogReplicationModelMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param remoteDestinations Clusters which the logicalGroup should be replicated to
     */
    public void addDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations is null or empty."));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations contains null or empty elements."));
        List<String> finalRemoteDestinations = deduplicate(remoteDestinations);
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    DestinationInfoVal clientDestinations;

                    if (txn.isExists(LR_MODEL_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        List<String> newDestinations = new ArrayList<>(finalRemoteDestinations);
                        newDestinations.removeAll(clientDestinations.getDestinationIdsList());

                        if (newDestinations.isEmpty()) {
                            log.debug("[{}] {}", clientName, "All destinations already present.");
                            return null;
                        }

                        clientDestinations = clientDestinations.toBuilder().addAllDestinationIds(newDestinations).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addAllDestinationIds(finalRemoteDestinations).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();

                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to add destinations, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Unable to add destinations", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove destination from LogReplicationModelMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param destination The cluster from which the logicalGroup should be removed from all future replication
     */
    public void removeDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(destination), String.format("[%s] %s", clientName, "destination is null or empty."));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    if (txn.isExists(LR_MODEL_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        if (clientDestinationIdsList.remove(destination)) {
                            if (clientDestinationIdsList.size() == 0) {
                                txn.delete(sourceMetadataTable, clientInfoKey);
                            } else {
                                clientDestinationIds = clientDestinationIds.toBuilder()
                                        .clearDestinationIds()
                                        .addAllDestinationIds(clientDestinationIdsList)
                                        .build();
                                txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinationIds, null);
                            }
                            txn.commit();

                            return null;
                        }
                        log.debug("[{}] {}", clientName, "No matching destination found.");
                        return null;
                    }
                    throw new NoSuchElementException(String.format("Record not found for group: %s", logicalGroup));
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to remove destination, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Unable to remove destination", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove a list of destinations from LogReplicationModelMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param remoteDestinations The cluster from which the logicalGroup should be removed from all future replication
     */
    public void removeDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations is null or empty."));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations contains null or empty elements."));
        List<String> finalRemoteDestinations = deduplicate(remoteDestinations);
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    if (txn.isExists(LR_MODEL_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        int initialNumDestinations = clientDestinationIdsList.size();
                        clientDestinationIdsList.removeAll(finalRemoteDestinations);

                        if (clientDestinationIdsList.size() == initialNumDestinations) {
                            throw new NoSuchElementException(String.format("No matching destinations found for group: %s", logicalGroup));
                        } else if (clientDestinationIdsList.size() + finalRemoteDestinations.size() > initialNumDestinations) {
                            log.debug("[{}] {}", clientName, "Not all destinations to remove are present.");
                        }

                        if (clientDestinationIdsList.size() == 0) {
                            txn.delete(sourceMetadataTable, clientInfoKey);
                        } else {
                            clientDestinationIds = clientDestinationIds.toBuilder()
                                    .clearDestinationIds()
                                    .addAllDestinationIds(clientDestinationIdsList)
                                    .build();
                            txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinationIds, null);
                        }
                        txn.commit();

                        return null;
                    }
                    throw new NoSuchElementException(String.format("Record not found for group: %s", logicalGroup));
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to remove destinations, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Unable to remove destinations", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Print the current destinations for a logical group
     *
     * @param logicalGroup Group which will have its destinations printed
     */
    public void showDestinations(String logicalGroup) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = ClientDestinationInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    if (txn.isExists(LR_MODEL_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        System.out.printf("\n========== Destinations for %s ==========\n", logicalGroup);
                        for (String destination : clientDestinationIdsList) {
                            System.out.println(destination);
                        }
                        System.out.println("==================================\n");

                        txn.close();
                        return null;
                    }
                    throw new NoSuchElementException(String.format("Record not found for group: %s", logicalGroup));
                } catch (TransactionAbortedException tae) {
                    log.error("[{}] {}: {}", clientName, "Unable to show destinations, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("[{}] {}: {}", clientName, "Unable to show destinations", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private boolean isValid(final Object obj) {
        if (obj == null) {
            return false;
        } else if (obj instanceof Collection) {
            return !((Collection<?>) obj).isEmpty();
        } else {
            return !Strings.isNullOrEmpty(obj.toString());
        }
    }

    private boolean hasNoNullOrEmptyElements(final Collection<?> collection) {
        if (collection == null) {
            return false;
        }
        for (Object obj : collection) {
            if (obj == null || (obj instanceof String && ((String) obj).isEmpty())) {
                return false;
            }
        }
        return true;
    }

    private List<String> deduplicate(List<String> list) {
        int initialSize = list.size();
        list = new ArrayList<>(new HashSet<>(list));
        if (initialSize != list.size()) {
            log.debug("[{}] {}", clientName, "Duplicate elements removed from list.");
        }
        return list;
    }
}
