package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.protobuf.StringValue;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.SnapshotSyncPluginValue;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.junit.Assert;

import java.util.UUID;

/**
 * Default Snapshot Sync Plugin
 *
 * This implementation returns immediately for start and end of a snapshot sync.
 */
@Slf4j
public class DefaultSnapshotSyncPlugin implements ISnapshotSyncPlugin {

    public static final String NAMESPACE = "corfu-IT";
    public static final String TABLE_NAME = "dummyTable";
    public static final String TAG = "snapshotSyncPlugin";
    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
    public static final String ON_START_VALUE = "Hello I executed the start! Checkpoint is freezed!";
    public static final String ON_END_VALUE = "I executed the end! Checkpoint unfreezed, bye!";

    @Override
    public void onSnapshotSyncStart(CorfuRuntime runtime) {
        try {
            updateDummyTable(runtime, ON_START_VALUE);
            log.debug("onSnapshotSyncStart :: OK");
        } catch (Exception e) {
            log.error("Error while attempting to start snapshot sync");
            Assert.fail();
        }
    }

    @Override
    public void onSnapshotSyncEnd(CorfuRuntime runtime) {
        try {
            updateDummyTable(runtime, ON_END_VALUE);
            log.debug("onSnapshotSyncEnd :: OK");
        } catch (Exception e) {
            log.error("Error while attempting to end snapshot sync");
            Assert.fail();
        }
    }

    private void updateDummyTable(CorfuRuntime runtime, String updateValue) {
        try {
            CorfuStore store = new CorfuStore(runtime);
            Table<ExampleSchemas.Uuid, SnapshotSyncPluginValue, SnapshotSyncPluginValue> table = store.openTable(NAMESPACE, TABLE_NAME,
                    ExampleSchemas.Uuid.class, SnapshotSyncPluginValue.class,
                    SnapshotSyncPluginValue.class, TableOptions.fromProtoSchema(SnapshotSyncPluginValue.class));
            IRetry.build(IntervalRetry.class, () -> {
                try(TxnContext txn = store.txn(NAMESPACE)) {
                    txn.putRecord(table, ExampleSchemas.Uuid.newBuilder()
                                    .setLsb(DEFAULT_UUID.getLeastSignificantBits())
                                    .setMsb(DEFAULT_UUID.getMostSignificantBits())
                                    .build(),
                            SnapshotSyncPluginValue.newBuilder().setValue(updateValue).build(),
                            SnapshotSyncPluginValue.newBuilder().setValue(updateValue).build());
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to connect to update dummy table in onSnapshotSyncStart.", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
            log.debug("onSnapshotSyncStart :: OK");
        } catch (Exception e) {
            log.error("Error while attempting to start snapshot sync");
            Assert.fail();
        }
    }
}
