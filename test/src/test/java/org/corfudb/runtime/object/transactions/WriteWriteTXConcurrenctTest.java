package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by dmalkhi on 12/13/16.
 */
public class WriteWriteTXConcurrenctTest extends TXConflictScenarios {

    // this is defined as abstract in AbstractTransactionContextTest
    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }

    @Test
    public void testOpacityInterleaved() throws Exception {
        // invoke the interleaving engine
        testOpacityWW(true);
    }

    @Test
    public void testOpacityThreaded() throws Exception {
        // invoke the threaded engine
        testOpacityWW(false);
    }


    public void testOpacityWW(boolean testInterleaved) throws Exception {
        testOpacity(testInterleaved);

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

    /**
     *  If task k aborts, then either task (k-1), or (k+1), or both, must have committed
     *  (wrapping around for tasks n-1 and 0, respectively).
     */
    public void testRWConflictWW(boolean testInterleaved) throws Exception {

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
    public void testRWConflictWWInterleaved() throws Exception {
        testRWConflictWW(true);
    }

    @Test
    public void testRWConflictWWThreaded() throws Exception {
        testRWConflictWW(false);
    }

    @Test
    public void testNoWriteConflictSimpleWW() throws Exception {
        testNoWriteConflictSimple();

        // both transactions should commit, no WW conflict
        assertThat(commitStatus.get(0))
                .isEqualTo(COMMITVALUE);
        assertThat(commitStatus.get(1))
                .isEqualTo(COMMITVALUE);
    }

    public void testAbortWW(boolean testInterleaved)
            throws Exception
    {
        concurrentAbortTest(testInterleaved);

        // calculate how many false abort might happen due to collisions in hashCode()
        int falseaborts = 0;
        Set<Integer> falsecollisions = new HashSet<>();
        for (int i = 0; i < numTasks; i++ )
            if (! falsecollisions.add(Integer.toString(i).hashCode()) ) falseaborts++;

        int aborts = 0;
        for (int i = 0; i < numTasks; i++)
            if (commitStatus.get(i) != COMMITVALUE)
                aborts++;

        assertThat(aborts)
                .isEqualTo(falseaborts);
    }

    @Test
    public void testAbortWWInterleaved()
            throws Exception
    {
        testAbortWW(true);
    }

    @Test
    public void testAbortWWThreaded()
            throws Exception
    {
        testAbortWW(false);
    }
}
