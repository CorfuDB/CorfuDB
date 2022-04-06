package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class PersistentCorfuTableTest extends AbstractViewTest {

    PersistentCorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable;
    UUID streamId = CorfuRuntime.getStreamID("test");
    CorfuRuntime rt;

    private static final long SMALL_CACHE_SIZE = 3;
    private static final long LARGE_CACHE_SIZE = 50_000;

    private void setupSerializer() {
        ISerializer protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
        rt.getSerializers().registerSerializer(protoSerializer);
        TestSchema.Uuid defaultUuidInstance = TestSchema.Uuid.getDefaultInstance();
        String typeUrl = "type.googleapis.com/" +  defaultUuidInstance.getDescriptorForType().getFullName();
        ((ProtobufSerializer)rt.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE))
                .getClassMap().put(typeUrl, defaultUuidInstance.getClass());
    }

    private void openTable() {
        corfuTable = new PersistentCorfuTable$CORFUSMR<>();
        TypeToken typeToken = new TypeToken<PersistentCorfuTable<TestSchema.Uuid,
                CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>>>() {
        };
        Object[] args = {};
        corfuTable.setCorfuSMRProxy(new MVOCorfuCompileProxy(
                rt,
                streamId,
                typeToken.getRawType(),
                args,
                rt.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE),
                new HashSet<UUID>(),
                corfuTable
        ));
    }

    @Test
    public void testTxn() {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        TestSchema.Uuid key1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid metadata1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord value1 = new CorfuRecord(payload1, metadata1);

        rt.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.put(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        TestSchema.Uuid key2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid payload2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid metadata2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        CorfuRecord value2 = new CorfuRecord(payload2, metadata2);

        TestSchema.Uuid nonExistingKey = TestSchema.Uuid.newBuilder().setLsb(3).setMsb(3).build();

        rt.getObjectsView().TXBegin();

        // Put key2
        corfuTable.put(key2, value2);

        // Table should contain both key1 and key2, but not nonExistingKey
        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 0
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.get(key2)).isNull();
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 1
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 1))
                .build()
                .begin();

        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.size()).isEqualTo(2);
        rt.getObjectsView().TXEnd();
    }

    /**
     * Do not allow accessing a stale version which is older than the smallest
     * version of the same object in MVOCache
     */
    @Test
    public void testAccessStaleVersion() {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        // Create 4 versions of the table
        for (int i = 0; i < 4; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }

        // Populate the MVOCache with v1, v2 and v3.
        // v0 is evicted when v3 is put into the cache
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 3))
                .build()
                .begin();

        final TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(0);
        assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(0);
        rt.getObjectsView().TXEnd();

        // Accessing v0 is not allowed
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> corfuTable.get(key).getPayload())
                .withCauseInstanceOf(StaleObjectVersionException.class);

    }

    @Test
    public void simpleParallelAccess() throws InterruptedException {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        int readSize = 100;

        // 1st txn at v0 puts keys {0, .., readSize-1} into the table
        rt.getObjectsView().TXBegin();
        for (int i = 0; i < readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }
        rt.getObjectsView().TXEnd();

        // 2nd txn at v1 puts keys {readSize, ..., readSize*2-1} into the table
        rt.getObjectsView().TXBegin();
        for (int i = readSize; i < 2*readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }
        rt.getObjectsView().TXEnd();

        // Two threads doing snapshot read in parallel
        Thread t1 = new Thread(() -> snapshotRead(0, 0, readSize));
        Thread t2 = new Thread(() -> snapshotRead(1, readSize, 2*readSize));

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private void snapshotRead(long ts, int low, int high) {
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, ts))
                .build()
                .begin();
        for (int i = low; i < high; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(i);
            assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(i);
        }
        rt.getObjectsView().TXEnd();
    }

}
