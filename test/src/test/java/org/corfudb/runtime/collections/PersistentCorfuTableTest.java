package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.MVOCache;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Before;
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

    @Before
    public void setup() {
        TypeToken typeToken = new TypeToken<PersistentCorfuTable<TestSchema.Uuid,
                CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>>>() {};
        Object[] args = {};

        rt = getDefaultRuntime();

        ISerializer protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
        getDefaultRuntime().getSerializers().registerSerializer(protoSerializer);

        corfuTable = new PersistentCorfuTable$CORFUSMR<>();
        corfuTable.setCorfuSMRProxy(new MVOCorfuCompileProxy(
                rt,
                streamId,
                typeToken.getRawType(),
                args,
                protoSerializer,
                new HashSet<UUID>(),
                corfuTable
        ));
    }

    @Test
    public void testTxn() {
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

    @Test
    public void test() {
        TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord value = new CorfuRecord(payload, metadata);
        corfuTable.put(key, value);
        assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(payload.getLsb());
        assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(payload.getMsb());

        TestSchema.Uuid nonExistingKey = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        assertThat(corfuTable.get(nonExistingKey)).isNull();
    }
}
