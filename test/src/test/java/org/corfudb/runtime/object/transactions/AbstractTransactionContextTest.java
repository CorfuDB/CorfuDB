package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.junit.Before;
import org.junit.Test;

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
                    new TypeToken<SMRMap<String, String>>() {},
                    "test stream"
            );
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

    @Test
    public void ensureUserTsIsInherited() {
        TokenResponse resp = getRuntime().getSequencerView().next();
        getRuntime().getAddressSpaceView().write(resp, "data".getBytes());

        final Token parentTs = resp.getToken();

        getRuntime().getObjectsView().TXBuild()
                .snapshot(parentTs)
                .build()
                .begin();
        getRuntime().getObjectsView().TXBuild()
                .build()
                .begin();

        // Verify that the child inherits the user defined
        // snapshot from the parent
        assertThat(TransactionalContext
                .getCurrentContext()
                .getSnapshotTimestamp())
                .isEqualTo(parentTs);
        getRuntime().getObjectsView().TXEnd();
        assertThat(TransactionalContext
                .getCurrentContext()
                .getSnapshotTimestamp())
                .isEqualTo(parentTs);
        getRuntime().getObjectsView().TXEnd();
    }

    @Test
    public void ensureTsIsSetFromTail() {
        final long numTokens = 5;
        final long newTail = numTokens - 1;

        for (int x = 0; x < numTokens; x++) {
            getRuntime().getSequencerView().next();
        }

        getRuntime().getObjectsView().TXBuild()
                .build()
                .begin();
        getRuntime().getObjectsView().TXBuild()
                .build()
                .begin();

        // Verify that the child inherits timestamp
        // when a user defined snapshot isn't defined
        assertThat(TransactionalContext
                .getCurrentContext()
                .getSnapshotTimestamp().getSequence())
                .isEqualTo(newTail);
        getRuntime().getObjectsView().TXEnd();
        assertThat(TransactionalContext
                .getCurrentContext()
                .getSnapshotTimestamp().getSequence())
                .isEqualTo(newTail);
        getRuntime().getObjectsView().TXEnd();
    }

//    @Test
//    @Ignore("Verification is not in place.")
//    public void nestingUserDefineAndDefaultTs() {
//        // Let the parent transaction set its its ts
//        // from the sequencer
//        final Token childTs = new Token(0L, 5L);
//        getRuntime().getObjectsView()
//                .TXBuild()
//                .build()
//                .begin();
//        // nest a transaction with a user defined ts
//        // and verify that it fails
//        assertThatThrownBy(() -> getRuntime().getObjectsView()
//                .TXBuild()
//                .snapshot(childTs)
//                .build()
//                .begin())
//                .isInstanceOf(IllegalArgumentException.class);
//
//    }
//
//    @Test
//    @Ignore("Verification is not in place.")
//    public void invalidUserDefinedTs() {
//        final Token emptySlot = new Token(0L, 2);
//
//        assertThatThrownBy(() -> getRuntime().getObjectsView()
//                .TXBuild()
//                .snapshot(emptySlot)
//                .build()
//                .begin())
//                .isInstanceOf(IllegalArgumentException.class);
//
//
//        TokenResponse res = getRuntime().getSequencerView().next();
//        getRuntime().getAddressSpaceView().write(res, "data".getBytes());
//
//        // We construct an invalid token and try to start a new transaction
//        final Token invalidTs = new Token(res.getEpoch() + 1, res.getSequence());
//
//        assertThatThrownBy(() -> getRuntime().getObjectsView()
//                .TXBuild()
//                .snapshot(invalidTs)
//                .build()
//                .begin())
//                .isInstanceOf(IllegalArgumentException.class);
//    }
}
