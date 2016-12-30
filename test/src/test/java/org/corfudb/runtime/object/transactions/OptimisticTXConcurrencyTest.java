package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends TXConflictScenarios {

    @Override
    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.OPTIMISTIC)
                .begin();
    }

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
}
