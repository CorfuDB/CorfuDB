package org.corfudb.runtime.collections;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.PersistentCorfuTableTest.CacheSizeForTest;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.ManagedCorfuTableForTest;
import org.corfudb.test.TestSchema.Uuid;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuTableDynamicTest extends AbstractViewTest {

    @TestFactory
    public Stream<DynamicTest> dynamicTests() {
        List<GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>>> tables = new ArrayList<>();
        tables.add(new PersistentCorfuTable<>());
        tables.add(new PersistedCorfuTable<>());

        return tables.stream().map(table -> DynamicTest.dynamicTest("dynamic", () -> {
            resetTests();

            addSingleServer(SERVERS.PORT_0);

            buildNewManagedRuntime(getLargeRtParams(), rt -> {
                ManagedCorfuTableForTest.<Uuid, Uuid, Uuid>builder()
                        .rt(rt)
                        .kClass(Uuid.class)
                        .vClass(Uuid.class)
                        .mClass(Uuid.class)
                        .schemaOptions(null)
                        .table(table)
                        .build()
                        .execute(corfuTable -> {
                            for (int i = 0; i < 100; i++) {
                                Uuid uuidMsg = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                                CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(uuidMsg, uuidMsg);
                                corfuTable.insert(uuidMsg, value1);
                            }

                            AtomicInteger size1 = new AtomicInteger();
                            AtomicInteger size2 = new AtomicInteger();
                            Thread thread1 = new Thread(() -> {
                                rt.getObjectsView().TXBuild()
                                        .type(TransactionType.SNAPSHOT)
                                        .snapshot(new Token(0, 9))
                                        .build()
                                        .begin();
                                size1.set(corfuTable.keySet().size());
                                rt.getObjectsView().TXEnd();
                            });

                            Thread thread2 = new Thread(() -> {
                                rt.getObjectsView().TXBuild()
                                        .type(TransactionType.SNAPSHOT)
                                        .snapshot(new Token(0, 99))
                                        .build()
                                        .begin();
                                size2.set(corfuTable.keySet().size());
                                rt.getObjectsView().TXEnd();
                            });

                            thread1.start();
                            thread2.start();
                            thread1.join();
                            thread2.join();

                            assertThat(size1.get()).isEqualTo(10);
                            assertThat(size2.get()).isEqualTo(100);
                        });
            });

            cleanupBuffers();
        }));
    }

    private CorfuRuntimeParameters getLargeRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.LARGE.size)
                .build();
    }

    private CorfuRuntimeParameters getMediumRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.MEDIUM.size)
                .build();
    }

    private CorfuRuntimeParameters getSmallRtParams() {
        return CorfuRuntimeParameters.builder()
                .maxCacheEntries(CacheSizeForTest.SMALL.size)
                .build();
    }
}
