package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableList;
import lombok.val;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.corfutable.GetVersionedObjectOptimizationSpec;
import org.corfudb.runtime.collections.corfutable.MultiRuntimeSpec;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.CorfuTableSpec;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableConfigParams;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableProtobufConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetup;
import org.corfudb.test.managedtable.ManagedRuntime;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.corfudb.test.RtParamsForTest.getLargeRtParams;

public class CorfuTableDynamicTest extends AbstractViewTest {

    @TestFactory
    public Stream<DynamicTest> getVersionedObjectOptimizationSpec() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        return dynamicTest(
                getLargeRtParams(),
                ManagedCorfuTableProtobufConfig.buildUuid(),
                new GetVersionedObjectOptimizationSpec()
        );
    }

    @TestFactory
    public Stream<DynamicTest> multiRuntimeSpec() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        return dynamicTest(
                getLargeRtParams(),
                ManagedCorfuTableProtobufConfig.buildUuid(),
                new MultiRuntimeSpec()
        );
    }

    private <K, V> Stream<DynamicTest> dynamicTest(
            CorfuRuntimeParameters rtParams,
            ManagedCorfuTableConfig cfg,
            CorfuTableSpec<K, V> spec
    ) {

        val persistentParams = ManagedCorfuTableConfigParams.PERSISTENT_PROTOBUF_TABLE;
        val persistedParams = ManagedCorfuTableConfigParams.PERSISTED_PROTOBUF_TABLE;

        List<ManagedCorfuTableSetup<K, V>> tables = ImmutableList.of(
                ManagedCorfuTableSetupManager.getTableSetup(persistentParams),
                ManagedCorfuTableSetupManager.getTableSetup(persistedParams)
        );

        return tables.stream().map(tableSetup -> DynamicTest.dynamicTest(tableSetup.toString(), () -> {
                    ManagedRuntime managedRt = ManagedRuntime
                            .from(rtParams)
                            .setup(rt -> rt.parseConfigurationString(getDefaultConfigurationString()));

                    ManagedCorfuTable<K, V> managedTable = ManagedCorfuTable
                            .<K, V>build()
                            .config(cfg)
                            .managedRt(managedRt)
                            .tableSetup(tableSetup);

                    managedTable.execute(spec::test);
                    cleanupBuffers();
                })
        );
    }
}
