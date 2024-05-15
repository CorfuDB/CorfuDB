package org.corfudb.runtime.collections.corfutable;

import lombok.AllArgsConstructor;
import lombok.val;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.CacheSizeForTest;
import org.corfudb.test.CorfuTableSpec;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedRuntime;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class MultiRuntimeSpec extends AbstractViewTest implements CorfuTableSpec<Uuid, CorfuRecord<Uuid, Uuid>> {

    @Override
    public void test(CorfuTableSpecContext<Uuid, CorfuRecord<Uuid, Uuid>> ctx) throws Exception {
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

        CorfuRuntimeParameters rt2Params = CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.LARGE.size)
                .build();
        ManagedRuntime managedRt = ManagedRuntime
                .from(rt2Params)
                .setup(rt2 -> rt2.parseConfigurationString(getDefaultConfigurationString()));

        ManagedCorfuTable
                .<Uuid, CorfuRecord<Uuid, Uuid>>from(ctx.getConfig(), managedRt)
                .tableSetup(ManagedCorfuTableSetupManager.getTableSetup(ctx.getConfig().getParams()))
                .execute(ctx2 -> {
                    var ct = ctx2.getCorfuTable();
                    assertThat(ct.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
                    assertThat(ct.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
                    assertThat(ct.size()).isEqualTo(1);
                });
    }
}
