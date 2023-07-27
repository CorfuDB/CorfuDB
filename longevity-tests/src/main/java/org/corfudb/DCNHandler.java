package org.corfudb;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.TxnContext;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class DCNHandler extends NotificationListenerHandler {

    public DCNHandler(String name, CorfuStore corfuStore, CommonUtils commonUtils, String namespace, String streamTag) {
        super(name, corfuStore, commonUtils, namespace, streamTag);
        log.info("Initializing DCNHandler...");
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> {
            for (CorfuStreamEntry entry : entries) {
                try (TxnContext txn = store.txn(namespace)) {
                    if (schema.getTableName().contains("Queue")) {
                        continue;
                    }
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
