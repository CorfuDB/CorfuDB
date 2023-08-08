package org.corfudb;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class DCNHandler extends NotificationListenerHandler {

    public DCNHandler(String name, CorfuStore corfuStore, CommonUtils commonUtils, String namespace, String streamTag) {
        super(name, corfuStore, commonUtils, namespace, streamTag);
        log.info("Initializing DCNHandler...");
    }

    @Override
    void onEachSchema(TableSchema tableSchema, List<CorfuStreamEntry> entry) {
        //
    }

    @Override
    void onEachEntry(TableSchema tableSchema, CorfuStreamEntry entry) {
        if (tableSchema.getTableName().contains("Queue")) {
            return;
        }
        try (TxnContext txn = store.txn(namespace)) {
            int size = txn.count(tableSchema.getTableName());
            if (size > 5) {
                log.debug("TableName: {} has size: {}", tableSchema.getTableName(), size);
                List<CorfuStoreEntry<Message, Message, Message>> filteredEntries =
                        txn.getByIndex(tableSchema.getTableName(), "anotherKey",
                                ThreadLocalRandom.current().nextInt(100));
                filteredEntries.forEach(e -> txn.delete(tableSchema.getTableName(), e.getKey()));
            }
            txn.commit();
        } catch (Exception e) {
            log.error("Exception encountered in DCNHandler: " + e);
        }
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        results.getEntries().forEach((schema, entries) -> {
            if (schema.getTableName().contains("Queue")) {
                return;
            }
            try (TxnContext txn = store.txn(namespace)) {
                int size = txn.count(schema.getTableName());
                if (size > 5) {
                    log.debug("TableName: {} has size: {}", schema.getTableName(), size);
                    List<CorfuStoreEntry<Message, Message, Message>> filteredEntries =
                            txn.getByIndex(schema.getTableName(), "anotherKey",
                                    ThreadLocalRandom.current().nextInt(100));
                    filteredEntries.forEach(e -> txn.delete(schema.getTableName(), e.getKey()));
                }
                txn.commit();
            } catch (Exception e) {
                log.error("Exception encountered in DCNHandler: " + e);
            }
        });
    }

    @Override
    protected CorfuStoreMetadata.Timestamp performFullSync() {
        return null;
    }

    @Override
    public void onError(Throwable throwable) {
    }
}
