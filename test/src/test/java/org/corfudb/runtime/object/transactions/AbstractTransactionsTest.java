package org.corfudb.runtime.object.transactions;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.junit.Before;

import java.util.Collections;

/**
 * Created by dmalkhi on 1/4/17.
 */
public abstract class AbstractTransactionsTest extends AbstractObjectTest {

    @Before
    public void becomeCorfuApp() {

        ServerContextBuilder serverContextBuilder = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setCompactionPolicyType("GARBAGE_SIZE_FIRST")
                .setSegmentGarbageRatioThreshold("0")
                .setSegmentGarbageSizeThresholdMB("0");

        getDefaultRuntime(serverContextBuilder);
    }

    /**
     * Utility method to start a (default type) TX;
     * must be overriden with the desired type of TX
     */
    public abstract void TXBegin();

    /**
     * Utility method to end a TX
     */
    protected long TXEnd() {
        return getRuntime().getObjectsView().TXEnd();
    }


    protected void TXAbort() {
        getRuntime().getObjectsView().TXAbort();
    }

    /**
     * Utility method to start an optimistic TX
     */
    protected void OptimisticTXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.OPTIMISTIC)
                .build()
                .begin();
    }

    /**
     * Utility method to start a snapshot TX
     */
    protected void SnapshotTXBeginWithTimestamp(long snapshotTimestamp) {
        Token t2 = new Token(0l, snapshotTimestamp);
        TokenResponse s2 = new TokenResponse(t2, Collections.emptyMap());

        if (getRuntime().getAddressSpaceView().peek(t2.getSequence()) == null) {
            byte[] data = "data".getBytes();
            getRuntime().getAddressSpaceView().write(s2, data);
        }
        
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0L, snapshotTimestamp))
                .build()
                .begin();
    }


    protected void SnapshotTXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();
    }

    /**
     * Utility method to start a write-write TX
     */
    protected void WWTXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }

}
