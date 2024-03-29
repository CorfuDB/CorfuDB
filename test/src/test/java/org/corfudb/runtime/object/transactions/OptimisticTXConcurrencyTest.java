package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends TXConflictScenariosTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

    @Test
    public void testAbortWWInterleaved()
            throws Exception
    {
        concurrentAbortTest(true);

        // no assertion, just print abort rate
    }

    @Test
    public void testAbortWWThreaded()
            throws Exception
    {
        concurrentAbortTest(false);

        // no assertion, just print abort rate
    }

    @Test
    public void checkRollbackNested()  throws Exception {
        ArrayList<ICorfuTable> maps = new ArrayList<>();

        final int nmaps = 2;
        for (int i = 0; i < nmaps; i++)
            maps.add( (ICorfuTable<Integer, String>) instantiateCorfuObject(
                    new TypeToken<PersistentCorfuTable<Integer, String>>() {}, "test stream" + i)
            );
        final int key1 = 1, key2 = 2, key3 = 3;
        final String tst1 = "foo", tst2 = "bar";
        final int nNests = PARAMETERS.NUM_ITERATIONS_LOW;

        // start tx in one thread, establish snapshot time
        t(1, () -> {
            TXBegin();
            maps.get(0).get(key1);
            maps.get(1).get(key1);
        });

        // in another thread, nest nNests transactions writing to different streams
        t(2, () -> {
            for (int i = 0; i < nNests; i++) {
                TXBegin();
                maps.get((i%nmaps)).insert(key1, (i % nmaps) == 0 ? tst1 : tst2);
                maps.get((i%nmaps)).insert(key2, (i % nmaps) == 0 ? tst1 : tst2);
                maps.get((i%nmaps)).insert(key3, (i % nmaps) == 0 ? tst1 : tst2);
            }
        });

        t(1, () -> {
            assertThat(maps.get(0).get(key1))
                    .isEqualTo(null);
            assertThat(maps.get(1).get(key1))
                    .isEqualTo(null);
        });

    }

    /**
     * This test evaluates a case of false hash conflict, i.e.,
     * the case where given two different keys (UUIDs) its hash codes conflict.
     */
    @Test
    public void concurrentTransactionsNonConflictingKeysSameHash() {
        // These values have been obtained from real conflicting scenarios.
        UUID streamID = UUID.fromString("a0a6f485-db5c-33a2-92b2-a1edb188e5c7");
        UUID key1 = UUID.fromString("01003000-0000-0cb5-0000-000000000001");
        UUID key2 = UUID.fromString("01003000-0000-0cb6-0000-000000000002");

        // Confirm key1 and key2 hash codes actually conflict
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());

        ICorfuTable<UUID, String> mapTest = getRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<UUID, String>>() {})
                .setStreamID(streamID)
                .open();
        mapTest.clear();

        t(1, this::TXBegin);

        t(1, () -> mapTest.insert(key1, "v1"));

        t(2, this::TXBegin);
        t(2, () -> mapTest.insert(key2, "v2"));

        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

}
