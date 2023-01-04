package org.corfudb.runtime;

import com.google.common.base.Strings;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.corfudb.runtime.LogReplication.ClientInfoKey;
import org.corfudb.runtime.LogReplication.SinksInfoVal;
import org.corfudb.runtime.LogReplication.LRClientId;
import org.corfudb.runtime.LogReplication.LRClientInfo;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class LogReplicationClient {

    public static final String LR_REGISTRY_TABLE_NAME = "LogReplicationRegistrationTable";
    public static final String LR_SOURCE_METADATA_TABLE_NAME = "LogReplicationSourceMetadataTable";

    /**
     * Wait interval between consecutive sync attempts to cap exponential back-off
     */
    private static final int SYNC_THRESHOLD = 120;

    private final CorfuStore corfuStore;
    private final Table<LRClientId, LRClientInfo, Message> registryTable;
    private final Table<ClientInfoKey, SinksInfoVal, Message> sourceMetadataTable;

    private final LRClientId clientKey;
    private final LRClientInfo clientInfo;

    private final String clientName;
    private final String modelEnum;

    private final Logger log;

    /**
     * Constructor for LogReplicationClient
     *
     * @param runtime Corfu Runtime
     * @param clientName String representation of the client name
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws IllegalAccessException IllegalAccessException
     */
    public LogReplicationClient(CorfuRuntime runtime, String clientName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        this.corfuStore = new CorfuStore(runtime);
        this.clientName = clientName;

        this.modelEnum = "LOGICAL_GROUPS";

        this.clientKey = LRClientId.newBuilder()
                .setClientName(clientName)
                .build();
        this.clientInfo = LRClientInfo.newBuilder()
                .setClientName(clientName)
                .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                .setRegistrationTime(Timestamp.newBuilder().setSeconds(new Date().getTime() / 1000).build())
                .build();

        this.log = LoggerFactory.getLogger("lr-client");

        this.registryTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRY_TABLE_NAME, LRClientId.class,
                LRClientInfo.class, null, TableOptions.builder().build());
        this.sourceMetadataTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME, ClientInfoKey.class,
                SinksInfoVal.class, null, TableOptions.builder().build());

        register();
    }

    /**
     * Adds client and model to LogReplicationRegistrationTable
     *
     * @return true if client was newly registered
     */
    public boolean register() {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    if (txn.isExists(LR_REGISTRY_TABLE_NAME, clientKey)) {
                        log.warn("Client already registered");
                        return false;
                    }
                    else {
                        txn.putRecord(registryTable, clientKey, clientInfo, null);
                        txn.commit();
                    }
                    return true;
                }
                catch (Exception e) {
                    log.error("Unable to register client {}, Error: {}", clientName, e);
                    return false;
                }
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

                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    SinksInfoVal clientDestinations;

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();

                        if (clientDestinations.getDestinationIdsList().contains(destination)) {
                            log.warn("Destination already added");
                            return false;
                        }

                        clientDestinations = clientDestinations.toBuilder().addDestinationIds(destination).build();
                    }
                    else {
                        clientDestinations = SinksInfoVal.newBuilder().addDestinationIds(destination).build();
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
        if (Strings.isNullOrEmpty(logicalGroup)) {
            log.error("Invalid logicalGroup");
            return false;
        }
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    SinksInfoVal clientDestinations;

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

                        for (String dest : remoteDestinations) {
                            if (clientDestinations.getDestinationIdsList().contains(dest)) {
                                return false;
                            }
                        }

                        clientDestinations = clientDestinations.toBuilder().addAllDestinationIds(remoteDestinations).build();
                    }
                    else {
                        clientDestinations = SinksInfoVal.newBuilder().addAllDestinationIds(remoteDestinations).build();
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

                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(logicalGroup)
                            .build();

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        SinksInfoVal clientDestinationIds = txn.getRecord(sourceMetadataTable, clientInfoKey).getPayload();
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
                        return false;
                    }
                    return false;
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
}
