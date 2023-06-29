package org.corfudb.runtime;

import com.google.common.base.Strings;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public abstract class LogReplicationClient {
    /**
     * Key: ClientRegistrationId
     * Value: ClientRegistrationInfo
     */
    public static final String LR_REGISTRATION_TABLE_NAME = "LogReplicationRegistrationTable";

    private Table<ClientRegistrationId, ClientRegistrationInfo, Message> replicationRegistrationTable;

    /**
     * Registers client for replication.
     *
     */
    public void register(CorfuStore corfuStore, String clientName, ReplicationModel model)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        ClientRegistrationId clientKey = ClientRegistrationId.newBuilder()
                .setClientName(clientName)
                .build();
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        ClientRegistrationInfo clientInfo = ClientRegistrationInfo.newBuilder()
                .setClientName(clientName)
                .setModel(model)
                .setRegistrationTime(timestamp)
                .build();

        try {
            replicationRegistrationTable = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME);
        } catch (NoSuchElementException | IllegalArgumentException e) {
            log.warn("Failed getTable operation, opening table.", e);
            replicationRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME,
                    ClientRegistrationId.class,
                    ClientRegistrationInfo.class,
                    null,
                    TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
        }

        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ClientRegistrationInfo clientRegistrationInfo = txn.getRecord(replicationRegistrationTable, clientKey).getPayload();

                    if (clientRegistrationInfo != null) {
                        log.warn(String.format("Client already registered.\n--- ClientRegistrationId ---\n%s" +
                                "--- ClientRegistrationInfo ---\n%s", clientKey, clientRegistrationInfo));
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

    protected boolean isValid(final Object obj) {
        if (obj == null) {
            return false;
        } else if (obj instanceof Collection) {
            return !((Collection<?>) obj).isEmpty();
        } else {
            return !Strings.isNullOrEmpty(obj.toString());
        }
    }

    protected boolean hasNoNullOrEmptyElements(final Collection<?> collection) {
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

    protected Set<String> deduplicate(List<String> list, String clientName) {
        int initialSize = list.size();
        Set<String> set = new HashSet<>(list);
        if (initialSize != set.size()) {
            log.info(String.format("[%s] Duplicate elements removed from list.", clientName));
        }
        return set;
    }
}
