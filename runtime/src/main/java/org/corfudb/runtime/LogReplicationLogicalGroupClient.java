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
import org.corfudb.runtime.LogReplication.ReplicationModel;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * A client to interface with log replication utilizing the logical groups replication model.
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

    private static final ReplicationModel model = ReplicationModel.LOGICAL_GROUPS;

    private Table<ClientRegistrationId, ClientRegistrationInfo, Message> replicationRegistrationTable;
    private Table<ClientDestinationInfoKey, DestinationInfoVal, Message> sourceMetadataTable;

    private final CorfuStore corfuStore;
    private final ClientRegistrationId clientKey;
    private final ClientRegistrationInfo clientInfo;
    private final String clientName;

    /**
     * Constructor for LogReplicationLogicalGroupClient
     *
     * @param runtime Corfu Runtime
     * @param clientName String representation of the client name
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws IllegalAccessException IllegalAccessException
     */
    public LogReplicationLogicalGroupClient(CorfuRuntime runtime, String clientName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Preconditions.checkArgument(isValid(clientName), String.format("%s", "clientName is null or empty."));

        this.corfuStore = new CorfuStore(runtime);
        this.clientName = clientName;
        this.clientKey = ClientRegistrationId.newBuilder()
                .setClientName(clientName)
                .build();
        Instant time = Instant.now();
        this.clientInfo = ClientRegistrationInfo.newBuilder()
                .setClientName(clientName)
                .setModel(model)
                .setRegistrationTime(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build())
                .build();

        try {
            this.replicationRegistrationTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            log.warn("Failed getTable operation, opening table.", e);
            this.replicationRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME, ClientRegistrationId.class,
                    ClientRegistrationInfo.class, null, TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
        }

        try {
            this.sourceMetadataTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            log.warn("Failed getTable operation, opening table.", e);
            this.sourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME, ClientDestinationInfoKey.class,
                    DestinationInfoVal.class, null, TableOptions.fromProtoSchema(DestinationInfoVal.class));
        }

        register();
    }

    /**
     * Adds client and model to LogReplicationRegistrationTable.
     */
    private void register() {
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientRegistrationInfo clientRegistrationInfo = txn.getRecord(replicationRegistrationTable, clientKey).getPayload();

                    if (clientRegistrationInfo != null) {
                        log.warn(String.format("Client already registered.\n--- ClientRegistrationId ---\n%s--- ClientRegistrationInfo ---\n%s", clientKey, clientRegistrationInfo));
                    } else {
                        txn.putRecord(replicationRegistrationTable, clientKey, clientInfo, null);
                    }

                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to register client.", clientName), tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Client registration failed.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Adds a destination to LogReplicationModelMetadataTable for monitoring.
     *
     * @param logicalGroup The group of tables to be replicated, represented as a string.
     * @param destination The clusters which the specified group will be replicated to.
     */
    public void addDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(destination), String.format("[%s] %s", clientName, "destination is null or empty."));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                    if (clientDestinations != null) {
                        if (clientDestinations.getDestinationIdsList().contains(destination)) {
                            txn.commit();
                            log.info(String.format("[%s] Destination already exists.", clientName));
                            return null;
                        }
                        clientDestinations = clientDestinations.toBuilder().addDestinationIds(destination).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addDestinationIds(destination).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to add destination, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to add destination.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Add a list of destinations for a group to the LogReplicationModelMetadataTable for monitoring.
     *
     * @param logicalGroup The group of tables to be replicated, represented as a string.
     * @param remoteDestinations The clusters which the specified group will be replicated to.
     */
    public void addDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations is null or empty."));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations contains null or empty elements."));
        List<String> finalRemoteDestinations = new ArrayList<>(deduplicate(remoteDestinations));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                    if (clientDestinations != null) {
                        finalRemoteDestinations.removeAll(clientDestinations.getDestinationIdsList());
                        if (finalRemoteDestinations.isEmpty()) {
                            txn.commit();
                            log.info(String.format("[%s] All destinations already present.", clientName));
                            return null;
                        }
                        clientDestinations = clientDestinations.toBuilder().addAllDestinationIds(finalRemoteDestinations).build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder().addAllDestinationIds(finalRemoteDestinations).build();
                    }

                    txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    txn.commit();
                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to add destinations, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to add destinations.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove a destination from the LogReplicationModelMetadataTable for a specific group.
     *
     * @param logicalGroup The group of tables that are replicated, represented as a string.
     * @param destination The cluster which should be removed from the logical group for all future replication.
     */
    public void removeDestination(String logicalGroup, String destination) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(destination), String.format("[%s] %s", clientName, "destination is null or empty."));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                    if (clientDestinations != null) {
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinations.getDestinationIdsList());

                        if (clientDestinationIdsList.remove(destination)) {
                            if (clientDestinationIdsList.size() == 0) {
                                txn.delete(sourceMetadataTable, clientInfoKey);
                            } else {
                                clientDestinations = clientDestinations.toBuilder()
                                        .clearDestinationIds()
                                        .addAllDestinationIds(clientDestinationIdsList)
                                        .build();
                                txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                            }
                        } else {
                            log.info(String.format("[%s] No matching destination found for: %s", clientName, destination));
                        }
                    } else {
                        log.warn(String.format("Record not found for group: %s", logicalGroup));
                    }

                    txn.commit();
                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to remove destination, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to remove destination.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove a list of destinations from the LogReplicationModelMetadataTable for a specific group.
     *
     * @param logicalGroup The group of tables that are replicated, represented as a string.
     * @param remoteDestinations The clusters which should be removed from the logical group for all future replication.
     */
    public void removeDestination(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        Preconditions.checkArgument(isValid(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations is null or empty."));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations), String.format("[%s] %s", clientName, "remoteDestinations contains null or empty elements."));
        Set<String> removeRemoteDestinationsSet = deduplicate(remoteDestinations);
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                    if (clientDestinations != null) {
                        Set<String> clientDestinationIdsSet = new HashSet<>(clientDestinations.getDestinationIdsList());
                        List<String> clientDestinationIdsList = clientDestinationIdsSet.stream()
                                .filter(i -> !removeRemoteDestinationsSet.contains(i))
                                .collect(Collectors.toList());

                        if (clientDestinationIdsList.size() < clientDestinationIdsSet.size()) {
                            if (clientDestinationIdsList.size() + removeRemoteDestinationsSet.size() > clientDestinationIdsSet.size()) {
                                removeRemoteDestinationsSet.removeAll(clientDestinationIdsSet);
                                log.info(String.format("[%s] Following destinations to remove are not present: %s", clientName, removeRemoteDestinationsSet));
                            }

                            if (clientDestinationIdsList.size() == 0) {
                                txn.delete(sourceMetadataTable, clientInfoKey);
                            } else {
                                clientDestinations = clientDestinations.toBuilder()
                                        .clearDestinationIds()
                                        .addAllDestinationIds(clientDestinationIdsList)
                                        .build();
                                txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                            }
                        } else {
                            log.info(String.format("[%s] No matching destinations found for: %s", clientName, removeRemoteDestinationsSet));
                        }
                    } else {
                        log.warn(String.format("Record not found for group: %s", logicalGroup));
                    }

                    txn.commit();
                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to remove destinations, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to remove destinations.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Get the destinations associated with a specific logical group.
     *
     * @param logicalGroup The group whose destinations will be returned.
     * @return The associated destinations as a list of strings.
     */
    public List<String> getDestinations(String logicalGroup) {
        Preconditions.checkArgument(isValid(logicalGroup), String.format("[%s] %s", clientName, "logicalGroup is null or empty."));
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                    txn.commit();

                    if (clientDestinations != null) {
                        return (List<String>) clientDestinations.getDestinationIdsList();
                    }

                    log.warn(String.format("Record not found for group: %s", logicalGroup));
                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to get destinations, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to get destinations.", clientName), e);
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
            if (obj == null || obj instanceof String && ((String) obj).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Set<String> deduplicate(List<String> list) {
        int initialSize = list.size();
        Set<String> set = new HashSet<>(list);
        if (initialSize != set.size()) {
            log.info(String.format("[%s] Duplicate elements removed from list.", clientName));
        }
        return set;
    }

    private ClientDestinationInfoKey buildClientDestinationInfoKey(String logicalGroup) {
        return ClientDestinationInfoKey.newBuilder()
                .setClientName(clientName)
                .setModel(model)
                .setGroupName(logicalGroup)
                .build();
    }
}
