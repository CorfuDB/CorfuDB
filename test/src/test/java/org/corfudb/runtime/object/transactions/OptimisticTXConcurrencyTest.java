package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends TXConflictScenariosTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }



    public void testOpacityOptimistic(boolean isInterleaved) throws Exception {

        testOpacity(isInterleaved);

        // verfiy that all aborts are justified
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

        // verfiy that all aborts are justified
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
}
