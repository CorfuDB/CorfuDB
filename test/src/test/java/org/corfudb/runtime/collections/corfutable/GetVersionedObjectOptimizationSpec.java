package org.corfudb.runtime.collections.corfutable;

import lombok.AllArgsConstructor;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.test.TestSchema.Uuid;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@AllArgsConstructor
public class GetVersionedObjectOptimizationSpec implements CorfuTableSpec {
    private final GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>> corfuTable;
    private final CorfuRuntime rt;

    public void test() throws Exception {

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
    }
}
