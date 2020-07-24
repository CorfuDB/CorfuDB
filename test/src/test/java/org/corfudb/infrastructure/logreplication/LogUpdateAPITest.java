package org.corfudb.infrastructure.logreplication;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LogUpdateAPITest extends AbstractViewTest {
    static final private int NUM_KEYS = 10;


    /**
     * Test the TxBuilder logUpdate API work properly.
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

        //start runtime 1, populate some data for table A, table C
        CorfuRuntime runtime1 = getDefaultRuntime().setTransactionLogging(true).connect();
        CorfuStore corfuStore1 = new CorfuStore(runtime1);

        Table<Uuid, Uuid, Uuid> tableA = corfuStore1.openTable(namespace, tableAName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());


        UUID uuidA = CorfuRuntime.getStreamID(tableA.getFullyQualifiedTableName());

        //update tableA
        for (int i = 0; i < NUM_KEYS; i ++) {
            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            corfuStore1.tx(namespace).update(tableAName, key, key, key).commit();
        }

        //start runtime 2, open A, B as a stream and C as an UFO
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        CorfuStore corfuStore2 = new CorfuStore(runtime2);
        Table<Uuid, Uuid, Uuid> tableC2 = corfuStore2.openTable(namespace, tableCName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream streamA = (new OpaqueStream(runtime2, runtime2.getStreamsView().
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
            CorfuStoreMetadata.Timestamp timestamp = corfuStore2.getTimestamp();
            TxBuilder txBuilder = corfuStore2.tx(namespace);

            //runtime2.getObjectsView().TXBegin();

            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            txBuilder.update(tableCName, key, key, key);
            OpaqueEntry opaqueEntry = iterator.next();
            for( SMREntry smrEntry : opaqueEntry.getEntries().get(uuidA)) {
                    txBuilder.logUpdate(CorfuRuntime.getStreamID(tableB.getFullyQualifiedTableName()), smrEntry);
            }
            txBuilder.commit(timestamp);
        }

        //verify data at B and C with runtime 1
        txStream.seek(tail);
        Iterator<ILogData> iterator1 = txStream.streamUpTo(runtime2.getAddressSpaceView().getLogTail()).iterator();
        while(iterator1.hasNext()) {
            ILogData data = iterator1.next();
            data.getStreams().contains(uuidB);
        }
        System.out.print("\nstreamBTail " + runtime2.getAddressSpaceView().getAllTails().getStreamTails().get(uuidB));


        Query q = corfuStore1.query(namespace);
        Set<Uuid> aSet = q.keySet(tableAName, null);
        Set<Uuid> bSet = q.keySet(tableBName, null);

        System.out.print("\naSet " + aSet + "\n\nbSet " + bSet);
        assertThat(bSet.containsAll(aSet)).isTrue();
        assertThat(aSet.containsAll(bSet)).isTrue();
    }
}