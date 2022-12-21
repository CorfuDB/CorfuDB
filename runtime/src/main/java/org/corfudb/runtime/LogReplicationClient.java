package org.corfudb.runtime;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.LogReplication.ClientInfo;
import org.corfudb.runtime.LogReplication.ClientInfoKey;
import org.corfudb.runtime.LogReplication.SinksInfoVal;
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
import java.util.List;
import java.util.stream.Collectors;

public class LogReplicationClient {

    public static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";
    public static final String REGISTRATION_TABLE_NAME = "LogReplicationRegistrationTable";
    public static final String LR_SOURCE_METADATA_TABLE_NAME = "LogReplicationSourceMetadataTable";

    private static final int SYNC_THRESHOLD = 120;

    private final CorfuStore corfuStore;

    private final String clientName;
    private final String modelEnum;

    private final Logger log;

    public LogReplicationClient(CorfuRuntime runtime, String clientName) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        this.corfuStore = new CorfuStore(runtime);
        this.clientName = clientName;

        // TODO: auto assign model_enum based on the client name
        this.modelEnum = "LOGICAL_GROUPS";

        this.log = LoggerFactory.getLogger("lr-client");

        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REGISTRATION_TABLE_NAME, StringKey.class,
                StringKey.class, null, TableOptions.builder().build());
    }

    public boolean registerReplicationClient() {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {

                    StringKey clientKey = StringKey.newBuilder()
                            .setKey(clientName)
                            .build();

                    ClientInfo clientInfo = ClientInfo.newBuilder()
                            .setClientName(clientName)
                            .build();

                    Table<StringKey, ClientInfo, Message> table = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, REGISTRATION_TABLE_NAME);

                    if (txn.isExists(REGISTRATION_TABLE_NAME, clientKey)) {
                        log.error("Client already registered");
                        return false;
                    }
                    else {
                        txn.putRecord(table, clientKey, clientInfo, null);
                        txn.commit();
                    }

                    return txn.isExists(REGISTRATION_TABLE_NAME, clientKey);
                }
                catch (Exception e) {
                    log.error("Unable to register client", e);
                    return false;
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    public boolean addDestination(String domain, String destination) {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {

                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(domain)
                            .build();

                    SinksInfoVal clientDestinations;

                    Table<ClientInfoKey, SinksInfoVal, Message> table = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME);

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(table, clientInfoKey).getPayload();

                        if (clientDestinations.getDestinationIdsList().contains(destination)) {
                            log.error("Destination already added");
                            return false;
                        }

                        clientDestinations.toBuilder().addDestinationIds(destination).build();
                    }
                    else {
                        clientDestinations = SinksInfoVal.newBuilder().addDestinationIds(destination).build();
                    }

                    txn.putRecord(table, clientInfoKey, clientDestinations, null);
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

    public boolean addDestination(String domain, List<String> remoteDestinations) {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(domain)
                            .build();

                    SinksInfoVal clientDestinations;
                    List<String> distinctRemoteDestinations = remoteDestinations
                            .stream()
                            .distinct()
                            .collect(Collectors.toList());

                    Table<ClientInfoKey, SinksInfoVal, Message> table = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME);

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        clientDestinations = txn.getRecord(table, clientInfoKey).getPayload();

                        for (String dest : distinctRemoteDestinations) {
                            if (clientDestinations.getDestinationIdsList().contains(dest)) {
                                return false;
                            }
                        }

                        clientDestinations.toBuilder().addAllDestinationIds(distinctRemoteDestinations).build();
                    }
                    else {
                        clientDestinations = SinksInfoVal.newBuilder().addAllDestinationIds(distinctRemoteDestinations).build();
                    }

                    txn.putRecord(table, clientInfoKey, clientDestinations, null);
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

    public boolean removeDestination(String domain, String destination) {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {

                    ClientInfoKey clientInfoKey = ClientInfoKey.newBuilder()
                            .setClientName(clientName)
                            .setModel(LogReplication.ReplicationModel.valueOf(modelEnum))
                            .setGroupName(domain)
                            .build();

                    Table<ClientInfoKey, SinksInfoVal, Message> table = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_SOURCE_METADATA_TABLE_NAME);

                    if (txn.isExists(LR_SOURCE_METADATA_TABLE_NAME, clientInfoKey)) {
                        SinksInfoVal clientDestinationIds = txn.getRecord(table, clientInfoKey).getPayload();
                        List<String> clientDestinationIdsList = new ArrayList<>(clientDestinationIds.getDestinationIdsList());

                        if (clientDestinationIdsList.remove(destination)) {
                            clientDestinationIds = clientDestinationIds.toBuilder()
                                    .clearDestinationIds()
                                    .addAllDestinationIds(clientDestinationIdsList)
                                    .build();
                            txn.putRecord(table, clientInfoKey, clientDestinationIds, null);
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
