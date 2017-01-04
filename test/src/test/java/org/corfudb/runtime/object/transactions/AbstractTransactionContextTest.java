package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.fail;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/21/16.
 */
public abstract class AbstractTransactionContextTest extends AbstractViewTest {

    protected ISMRMap<String, String> testMap;

    abstract void TXBegin();

    long TXEnd() {
        return getRuntime().getObjectsView().TXEnd();
    }

    @Before
    public void resetMap() {
        testMap = null;
        getDefaultRuntime();
    }

    public ISMRMap<String, String> getMap() {
        if (testMap == null) {
            testMap = getRuntime()
                    .getObjectsView()
                    .build()
                    .setStreamName("test stream")
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .open();
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
            sharedCounters.add(i, getRuntime().getObjectsView()
                    .build()
                    .setStreamName("test"+i)
                    .setType(CorfuSharedCounter.class)
                    .open() );

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
        long result = TXEnd();
        LogData ld =
                getRuntime()
                        .getAddressSpaceView()
                        .read(0);
        assertThat(ld.getType())
                .isEqualTo(DataType.EMPTY);
        assertThat(result)
                .isEqualTo(AbstractTransactionalContext.NOWRITE_ADDRESS);
    }

}
