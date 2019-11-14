package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends TXConflictScenariosTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }



    public void testOpacityOptimistic(boolean isInterleaved) throws Exception {

        testOpacity(isInterleaved);

        // verify that all aborts are justified
        for (int task_num = 0; task_num < numTasks; task_num++) {
            if (commitStatus.get(task_num) != COMMITVALUE) {
                assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                        .isNotEqualTo(snapStatus.get(task_num));
                assertThat(commitStatus.get((task_num + 1) % numTasks))
                        .isEqualTo(COMMITVALUE);
            }
        }
    }

    @Test
    public void testOpacityInterleaved() throws Exception {
        // invoke the interleaving engine
        testOpacityOptimistic(true);
    }

    @Test
    public void testOpacityThreaded() throws Exception {
        // invoke the threaded engine
        testOpacityOptimistic(false);
    }

    /**
     *  If task k aborts, then either task (k-1), or (k+1), or both, must have committed
     *  (wrapping around for tasks n-1 and 0, respectively).
     */
    public void testOptimism(boolean testInterleaved) throws Exception {

       testRWConflicts(testInterleaved);

        // verify that all aborts are justified
        for (int task_num = 0; task_num < numTasks; task_num++) {
            if (commitStatus.get(task_num) != COMMITVALUE)
                assertThat(commitStatus.get((task_num + 1) % numTasks) == COMMITVALUE ||
                        commitStatus.get((task_num - 1) % numTasks) == COMMITVALUE)
                        .isTrue();
        }

    }

    @Test
    public void testOptimismInterleaved() throws Exception {
        testOptimism(true);
    }

    @Test
    public void testOptimismThreaded() throws Exception {
        testOptimism(false);
    }

    @Test
    public void testNoWriteConflictSimpleOptimistic() throws Exception {
        testNoWriteConflictSimple();
        assertThat(commitStatus.get(0) == COMMITVALUE || commitStatus.get(1) == COMMITVALUE)
                .isTrue();
    }

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
        ArrayList<Map> maps = new ArrayList<>();

        final int nmaps = 2;
        for (int i = 0; i < nmaps; i++)
            maps.add( (SMRMap<Integer, String>) instantiateCorfuObject(
                    new TypeToken<SMRMap<Integer, String>>() {}, "test stream" + i)
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
                maps.get((i%nmaps)).put(key1, (i % nmaps) == 0 ? tst1 : tst2);
                maps.get((i%nmaps)).put(key2, (i % nmaps) == 0 ? tst1 : tst2);
                maps.get((i%nmaps)).put(key3, (i % nmaps) == 0 ? tst1 : tst2);
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

        Map<UUID, String> mapTest = getRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<UUID, String>>() {})
                .setStreamID(streamID)
                .open();
        mapTest.clear();

        t(1, this::TXBegin);

        t(1, () -> mapTest.put(key1, "v1"));

        t(2, this::TXBegin);
        t(2, () -> mapTest.put(key2, "v2"));

        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

}
