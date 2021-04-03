package org.corfudb.infrastructure.logreplication;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

@Slf4j
public class LogUpdateAPITest extends AbstractViewTest {
    static final private int NUM_KEYS = 10;

    /**
     * Test the TxnContext logUpdate API work properly.
     * It first populate tableA with some data. Then read tableA with stream API,
     * then apply the smrEntries to tableB with logUpdate API.
     * Verify that tableB contains all the keys that A has.
     *
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testUFOWithLogUpdate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String namespace = "default_namespace";
        String tableAName = "tableA";
        String tableBName = "tableB";
        String tableCName = "tableC";

        // Start runtime 1, populate some data for table A, table C
        CorfuRuntime runtime1 = getDefaultRuntime().setTransactionLogging(true).connect();
        CorfuStore corfuStore1 = new CorfuStore(runtime1);

        Table<Uuid, Uuid, Uuid> tableA = corfuStore1.openTable(namespace, tableAName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        UUID uuidA = CorfuRuntime.getStreamID(tableA.getFullyQualifiedTableName());

        // Update tableA
        for (int i = 0; i < NUM_KEYS; i ++) {
            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            TxnContext txnContext = corfuStore1.txn(namespace);
            txnContext.putRecord(tableA, key, key, key);
            txnContext.commit();
        }

        // Start runtime 2, open A, B as a stream and C as an UFO
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        CorfuStore corfuStore2 = new CorfuStore(runtime2);
        Table<Uuid, Uuid, Uuid> tableC2 = corfuStore2.openTable(namespace, tableCName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream streamA = (new OpaqueStream(runtime2.getStreamsView().
                get(uuidA, options))).streamUpTo(runtime2.getAddressSpaceView().getLogTail());

        IStreamView txStream = runtime2.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, StreamOptions.builder()
                        .cacheEntries(false)
                        .build());
        long tail = runtime2.getAddressSpaceView().getLogTail();

        Iterator<OpaqueEntry> iterator = streamA.iterator();

        Table<Uuid, Uuid, Uuid> tableB = corfuStore1.openTable(namespace, tableBName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        UUID uuidB = CorfuRuntime.getStreamID(tableB.getFullyQualifiedTableName());

        while (iterator.hasNext()) {
            Token token = runtime2.getSequencerView().query().getToken();
            CorfuStoreMetadata.Timestamp timestamp = CorfuStoreMetadata.Timestamp.newBuilder()
                    .setEpoch(token.getEpoch())
                    .setSequence(token.getSequence())
                    .build();
            TxnContext txnContext = corfuStore2.txn(namespace, IsolationLevel.snapshot(timestamp));

            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            txnContext.putRecord(tableC2, key, key, key);
            OpaqueEntry opaqueEntry = iterator.next();
            for( SMREntry smrEntry : opaqueEntry.getEntries().get(uuidA)) {
                    txnContext.logUpdate(CorfuRuntime.getStreamID(tableB.getFullyQualifiedTableName()), smrEntry);
            }
            txnContext.commit();
        }

        // Verify data at B and C with runtime 1
        txStream.seek(tail);
        Iterator<ILogData> iterator1 = txStream.streamUpTo(runtime2.getAddressSpaceView().getLogTail()).iterator();
        while (iterator1.hasNext()) {
            ILogData data = iterator1.next();
            data.getStreams().contains(uuidB);
        }

        try (TxnContext txn = corfuStore1.txn(namespace)) {
            Set<Uuid> aSet = txn.keySet(tableAName);
            Set<Uuid> bSet = txn.keySet(tableBName);
            assertThat(bSet.containsAll(aSet)).isTrue();
            assertThat(aSet.containsAll(bSet)).isTrue();
            txn.commit();
        }
    }
}