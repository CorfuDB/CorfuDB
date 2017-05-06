package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/21/16.
 */
public abstract class AbstractTransactionContextTest extends AbstractTransactionsTest {

    protected ISMRMap<String, String> testMap;

    @Before
    public void resetMap() {
        testMap = null;
    }

    public ISMRMap<String, String> getMap() {
        if (testMap == null) {
            testMap = (ISMRMap<String, String>) instantiateCorfuObject(
                    new TypeToken<SMRMap<String, String>>() {
                    }, "test stream"
            ) ;
        }
        return testMap;
    }

    String put(String key, String value) {
        return getMap().put(key, value);
    }

    String get(String key) {
        return getMap().get(key);
    }

    // Write only, don't read previous value.
    void write(String key, String value) {
        getMap().blindPut(key, value);
    }

    // counter-array tests below

    static final int INITIAL = 32;
    static final int OVERWRITE_ONCE = 33;
    static final int OVERWRITE_TWICE = 34;

    int numTasks;
    ArrayList<CorfuSharedCounter> sharedCounters;
    AtomicIntegerArray commitStatus;
    final int COMMITVALUE = 1; // by default, ABORTVALUE = 0
    AtomicIntegerArray snapStatus;

    /**
     * build an array of shared counters for the test
     */
    void setupCounters() {

        numTasks = PARAMETERS.NUM_ITERATIONS_MODERATE;
        sharedCounters = new ArrayList<>();

        for (int i = 0; i < numTasks; i++)
            sharedCounters.add(i,
                    instantiateCorfuObject(CorfuSharedCounter.class, "test"+i)
            );

        // initialize all shared counters
        for (int i = 0; i < numTasks; i++)
            sharedCounters.get(i).setValue(INITIAL);

        commitStatus = new AtomicIntegerArray(numTasks);
        snapStatus = new AtomicIntegerArray(numTasks);
    }

    /** Ensure that empty write sets are not written to the log.
     * This test applies to all contexts which is why it is in
     * the abstract test.
     */
    @Test
    public void ensureEmptyWriteSetIsNotWritten() {
        TXBegin();
        long result = getRuntime().getObjectsView().TXEnd();
        ILogData ld =
                getRuntime()
                        .getAddressSpaceView()
                        .peek(0);
        assertThat(ld)
                .isNull();

        assertThat(result)
                .isEqualTo(AbstractTransactionalContext.NOWRITE_ADDRESS);
    }

}
