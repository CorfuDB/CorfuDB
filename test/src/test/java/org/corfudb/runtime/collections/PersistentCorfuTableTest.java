package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.PersistentCorfuCompileProxy;
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

public class PersistentCorfuTableTest extends AbstractViewTest {

    PersistentCorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable;
    UUID streamId = CorfuRuntime.getStreamID("test");

    @Before
    public void setup() {
        TypeToken typeToken = new TypeToken<PersistentCorfuTable<TestSchema.Uuid,
                CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>>>() {};
        Object[] args = {};

        ISerializer protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
        getDefaultRuntime().getSerializers().registerSerializer(protoSerializer);

        corfuTable = new PersistentCorfuTable$CORFUSMR<>();
        corfuTable.setCorfuSMRProxy(new PersistentCorfuCompileProxy(
                getDefaultRuntime(),
                streamId,
                typeToken.getRawType(),
                args,
                protoSerializer,
                new HashSet<UUID>(),
                corfuTable
        ));
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
