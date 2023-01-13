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
import java.util.*;

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
    public static final String LR_REGISTRY_TABLE_NAME = "LogReplicationRegistrationTable";

    /**
     * Key: ClientDestinationInfoKey
     * Value: DestinationInfoVal
     */
    public static final String LR_SOURCE_METADATA_TABLE_NAME = "LogReplicationSourceMetadataTable";

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
            tempReplicationRegistrationTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRY_TABLE_NAME);
        } catch (NoSuchElementException nse) {
            tempReplicationRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRY_TABLE_NAME, ClientRegistrationId.class,
                    ClientRegistrationInfo.class, null, TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
        }
        this.replicationRegistrationTable = tempReplicationRegistrationTable;

        try {
            tempSourceMetadataTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME);
        } catch (NoSuchElementException nse) {
            tempSourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME, ClientDestinationInfoKey.class,
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
                    if (txn.isExists(LR_REGISTRY_TABLE_NAME, clientKey)) {
                        log.warn("Client already registered \nclientKey: {} \nclientInfo: {}",
                                clientKey, txn.getRecord(replicationRegistrationTable, clientKey).getPayload());
                    } else {
                        txn.putRecord(replicationRegistrationTable, clientKey, clientInfo, null);
                        txn.commit();
                    }
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("Unable to register client %s, Error: ", clientName), tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Client registration failed");
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add destination to LogReplicationSourceMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param destination Cluster which the logicalGroup should be replicated to
     */
    public void addDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), "logicalGroup is null or empty");
        Preconditions.checkArgument(isValid(destination), "destination is null or empty");
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
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
                            throw new IllegalArgumentException("Destination already exists");
                        }

                        clientDestinations = clientDestinations.toBuilder().addDestinationIds(destination).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addDestinationIds(destination).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("Unable to add destination, will be retried", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unable to add destination");
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add a list destination to LogReplicationSourceMetadataTable for monitoring
     *
     * @param logicalGroup String representative of the group of tables to be replicated
     * @param remoteDestinations Clusters which the logicalGroup should be replicated to
     */
    public void addDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), "logicalGroup is null or empty");
        Preconditions.checkArgument(isValid(remoteDestinations), "remoteDestinations is null or empty");
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
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
                        throw new IllegalArgumentException("Invalid destination(s) found in list");
                    }

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        List<String> newDestinations = new ArrayList<>(remoteDestinations);
                        newDestinations.removeAll(clientDestinations.getDestinationIdsList());

                        if (newDestinations.isEmpty()) {
                            log.debug("All destinations already present");
                            return null;
                        }

                        clientDestinations = clientDestinations.toBuilder().addAllDestinationIds(newDestinations).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addAllDestinationIds(remoteDestinations).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();

                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error("Unable to add destinations, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unable to add destinations");
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove destination from LogReplicationSourceMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param destination The cluster from which the logicalGroup should be removed from all future replication
     */
    public void removeDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), "logicalGroup is null or empty");
        Preconditions.checkArgument(isValid(destination), "destination is null or empty");
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
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
                        throw new NoSuchElementException("No matching destination found");
                    }
                    throw new NoSuchElementException(String.format("Record not found for group: %s", logicalGroup));
                } catch (TransactionAbortedException tae) {
                    log.error("Unable to remove destination, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unable to remove destination");
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove a list of destinations from LogReplicationSourceMetadataTable
     *
     * @param logicalGroup String representative of the group of tables which are replicated
     * @param remoteDestinations The cluster from which the logicalGroup should be removed from all future replication
     */
    public void removeDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), "logicalGroup is null or empty");
        Preconditions.checkArgument(isValid(remoteDestinations), "remoteDestinations is null or empty");
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
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
                        throw new IllegalArgumentException("Invalid destination(s) found in list");
                    }

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        DestinationInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        int initialNumDestinations = clientDestinationIdsList.size();
                        clientDestinationIdsList.removeAll(remoteDestinations);

                        if (clientDestinationIdsList.size() == initialNumDestinations) {
                            throw new NoSuchElementException("No matching destinations found");
                        } else if (clientDestinationIdsList.size() + remoteDestinations.size() > initialNumDestinations) {
                            log.warn("Not all destinations to remove are present");
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
                    log.error("Unable to remove destinations, will be retried", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unable to remove destinations");
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private static boolean isValid(final Object obj) {
        if (obj == null) {
            return false;
        } else if (obj instanceof Collection) {
            return !((Collection<?>) obj).isEmpty();
        } else {
            return !Strings.isNullOrEmpty(obj.toString());
        }
    }
}
