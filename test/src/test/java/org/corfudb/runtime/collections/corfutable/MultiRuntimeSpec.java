package org.corfudb.runtime.collections.corfutable;

import lombok.AllArgsConstructor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.CacheSizeForTest;
import org.corfudb.test.CorfuTableSpec;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class MultiRuntimeSpec extends AbstractViewTest implements CorfuTableSpec<Uuid, Uuid, Uuid> {

    @Override
    public void test(CorfuTableSpecContext<Uuid, Uuid, Uuid> ctx) throws Exception {
        CorfuRuntime rt = ctx.getRt();
        GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>> corfuTable = ctx.getCorfuTable();


        Uuid key1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        Uuid payload1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        Uuid metadata1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(payload1, metadata1);

        rt.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.insert(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);

        CorfuRuntime rt2 = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.LARGE.size)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(rt2, new ProtobufSerializer(new ConcurrentHashMap<>()));

        ManagedCorfuTable.buildDefault(rt).execute(ct -> {
            assertThat(ct.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
            assertThat(ct.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
            assertThat(ct.size()).isEqualTo(1);
        });
    }
}
