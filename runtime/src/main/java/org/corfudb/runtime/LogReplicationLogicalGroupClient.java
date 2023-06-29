package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * A client to interface with log replication utilizing the logical groups replication model.
 * <p>
 * Client allows for concurrent access by multiple threads. CRUD operations with the same key
 * may succeed concurrently, leading to potential race conditions as retries are not deterministic.
 */
@Slf4j
public class LogReplicationLogicalGroupClient extends LogReplicationClient{
    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_LOGICAL_GROUP_CLIENT = "00000000-0000-0000-0000-0000000000001";

    /**
     * Key: ClientDestinationInfoKey
     * Value: DestinationInfoVal
     */
    public static final String LR_MODEL_METADATA_TABLE_NAME = "LogReplicationModelMetadataTable";

    private static final ReplicationModel model = ReplicationModel.LOGICAL_GROUPS;

    private Table<ClientDestinationInfoKey, DestinationInfoVal, Message> sourceMetadataTable;

    private final CorfuStore corfuStore;
    private final String clientName;

    /**
     * Constructor for the log replication client for logical groups.
     *
     * @param runtime Corfu Runtime.
     * @param clientName String representation of the client name. This parameter is case-sensitive.
     * @throws IllegalArgumentException If clientName is null or empty.
     * @throws InvocationTargetException InvocationTargetException.
     * @throws NoSuchMethodException NoSuchMethodException.
     * @throws IllegalAccessException IllegalAccessException.
     */
    public LogReplicationLogicalGroupClient(CorfuRuntime runtime, String clientName)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Preconditions.checkArgument(isValid(clientName), "clientName is null or empty.");

        this.corfuStore = new CorfuStore(runtime);
        // TODO (V2): client name cannot be customized as Sink side does not have a way to create sessions for now
        this.clientName = DEFAULT_LOGICAL_GROUP_CLIENT;

        try {
            this.sourceMetadataTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            log.warn("Failed getTable operation, opening table.", e);
            this.sourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME,
                    ClientDestinationInfoKey.class,
                    DestinationInfoVal.class,
                    null,
                    TableOptions.fromProtoSchema(DestinationInfoVal.class));
        }

        register(corfuStore, clientName, model);
    }

    /**
     * Adds one or more destinations to a given logical group.
     * <p>
     * A new logical group will be created if one does not already exist.
     * If remoteDestinations contains existing elements they will be skipped.
     *
     * @param logicalGroup String tag that identifies the group of tables to be replicated together.
     *                     This parameter is case-sensitive.
     * @param remoteDestinations The clusters which the specified group will be replicated to.
     *                           Destinations in the list are case-sensitive.
     * @throws IllegalArgumentException If logicalGroup or remoteDestinations is null or empty, or
     *                                  if remoteDestinations contains null or empty elements.
     */
    public void addDestinations(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup),
                String.format("[%s] logicalGroup is null or empty.", clientName));
        Preconditions.checkArgument(isValid(remoteDestinations),
                String.format("[%s] remoteDestinations is null or empty.", clientName));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations),
                String.format("[%s] remoteDestinations contains null or empty elements.", clientName));
        List<String> finalRemoteDestinations = new ArrayList<>(deduplicate(remoteDestinations, clientName));
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
                        clientDestinations = clientDestinations.toBuilder()
                                .addAllDestinationIds(finalRemoteDestinations)
                                .build();
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder()
                                .addAllDestinationIds(finalRemoteDestinations)
                                .build();
                    }
                    // Manually add stream tag to the write set of this txn, such that relevant LogEntryReaders could
                    // be able to track the corresponding opaque stream.
                    TransactionalContext.getRootContext().getWriteSetInfo().getStreamTags()
                            .add(ObjectsView.getLogicalGroupStreamTagInfo(clientName).getStreamId());
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
     * Sets the destinations for a given logical group to the provided list.
     * <p>
     * A new logical group will be created if one does not already exist.
     * Setting destinations for a logical group will overwrite existing destinations.
     * Can be used to clear existing destinations by passing in an empty list.
     * Clearing existing destinations will remove the logical group.
     *
     * @param logicalGroup String tag that identifies the group of tables to be replicated together.
     *                     This parameter is case-sensitive.
     * @param remoteDestinations The clusters which the specified group will be replicated to.
     *                           Destinations in the list are case-sensitive.
     * @throws IllegalArgumentException If logicalGroup is null or empty, if remoteDestinations is
     *                                  null, or if remoteDestinations contains null or empty elements.
     */
    public void setDestinations(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup),
                String.format("[%s] logicalGroup is null or empty.", clientName));
        Preconditions.checkArgument(remoteDestinations != null,
                String.format("[%s] remoteDestinations is null.", clientName));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations),
                String.format("[%s] remoteDestinations contains null or empty elements.", clientName));
        List<String> finalRemoteDestinations = new ArrayList<>(deduplicate(remoteDestinations, clientName));
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                    if (clientDestinations != null) {
                        log.info(String.format("[%s] Following destinations are being overwritten: %s",
                                clientName, clientDestinations.getDestinationIdsList()));
                    }

                    if (finalRemoteDestinations.isEmpty()) {
                        log.info(String.format("[%s] Empty logical group %s, will be removed.",
                                clientName, logicalGroup));
                        txn.delete(sourceMetadataTable, clientInfoKey);
                    } else {
                        clientDestinations = DestinationInfoVal.newBuilder()
                                .addAllDestinationIds(finalRemoteDestinations)
                                .build();
                        txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                    }
                    // Manually add stream tag to the write set of this txn, such that relevant LogEntryReaders could
                    // be able to track the corresponding opaque stream.
                    TransactionalContext.getRootContext().getWriteSetInfo().getStreamTags()
                            .add(ObjectsView.getLogicalGroupStreamTagInfo(clientName).getStreamId());
                    txn.commit();
                    return null;
                } catch (TransactionAbortedException tae) {
                    log.error(String.format("[%s] Unable to set destinations, will be retried.", clientName), tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error(String.format("[%s] Unable to set destinations.", clientName), e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Remove one or more destinations from a given logical group.
     * <p>
     * The logical group is deleted when no destinations remain.
     * Any destination to remove that is not present is skipped over.
     *
     * @param logicalGroup The group of tables that are replicated, represented as a string.
     *                     This parameter is case-sensitive.
     * @param remoteDestinations The clusters which should be removed from the logical group for
     *                           all future replication. Destinations in the list are case-sensitive.
     * @throws IllegalArgumentException If logicalGroup or remoteDestinations is null or empty, or
     *                                  if remoteDestinations contains null or empty elements.
     */
    public void removeDestinations(String logicalGroup, List<String> remoteDestinations) {
        Preconditions.checkArgument(isValid(logicalGroup),
                String.format("[%s] logicalGroup is null or empty.", clientName));
        Preconditions.checkArgument(isValid(remoteDestinations),
                String.format("[%s] remoteDestinations is null or empty.", clientName));
        Preconditions.checkArgument(hasNoNullOrEmptyElements(remoteDestinations),
                String.format("[%s] remoteDestinations contains null or empty elements.", clientName));
        Set<String> removeRemoteDestinationsSet = deduplicate(remoteDestinations, clientName);
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
                                log.info(String.format("[%s] Following destinations to remove are not present: %s",
                                        clientName, removeRemoteDestinationsSet));
                            }

                            if (clientDestinationIdsList.isEmpty()) {
                                txn.delete(sourceMetadataTable, clientInfoKey);
                            } else {
                                clientDestinations = clientDestinations.toBuilder()
                                        .clearDestinationIds()
                                        .addAllDestinationIds(clientDestinationIdsList)
                                        .build();
                                txn.putRecord(sourceMetadataTable, clientInfoKey, clientDestinations, null);
                            }
                        } else {
                            log.info(String.format("[%s] No matching destinations found for: %s",
                                    clientName, removeRemoteDestinationsSet));
                        }
                    } else {
                        log.warn(String.format("[%s] Record not found for group: %s",
                                clientName, logicalGroup));
                    }

                    // Manually add stream tag to the write set of this txn, such that relevant LogEntryReaders could
                    // be able to track the corresponding opaque stream.
                    TransactionalContext.getRootContext().getWriteSetInfo().getStreamTags()
                            .add(ObjectsView.getLogicalGroupStreamTagInfo(clientName).getStreamId());
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
     * Gets the destinations associated with a specific logical group.
     *
     * @param logicalGroup The group whose destinations will be returned.
     *                     This parameter is case-sensitive.
     * @return The associated destinations as a list of strings, null if logical group is not found.
     * @throws IllegalArgumentException If logicalGroup is null or empty.
     */
    public List<String> getDestinations(String logicalGroup) {
        Preconditions.checkArgument(isValid(logicalGroup),
                String.format("[%s] logicalGroup is null or empty.", clientName));
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientDestinationInfoKey clientInfoKey = buildClientDestinationInfoKey(logicalGroup);
                    DestinationInfoVal clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
                    txn.commit();

                    if (clientDestinations != null) {
                        return (List<String>) clientDestinations.getDestinationIdsList();
                    }

                    log.warn(String.format("[%s] Record not found for group: %s",
                            clientName, logicalGroup));
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

    private ClientDestinationInfoKey buildClientDestinationInfoKey(String logicalGroup) {
        return ClientDestinationInfoKey.newBuilder()
                .setClientName(clientName)
                .setModel(model)
                .setGroupName(logicalGroup)
                .build();
    }
}
