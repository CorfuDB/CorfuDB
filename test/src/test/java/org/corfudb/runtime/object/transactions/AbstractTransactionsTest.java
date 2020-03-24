package org.corfudb.runtime.object.transactions;

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
        getDefaultRuntime();
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
    protected void SnapshotTXBegin() {
        // By default, begin a snapshot at address 2L
        Token t2 = new Token(0l, 2l);
        TokenResponse s2 = new TokenResponse(t2, Collections.emptyMap());

        if (getRuntime().getAddressSpaceView().peek(t2.getSequence()) == null) {
            byte[] data = "data".getBytes();
            getRuntime().getAddressSpaceView().write(s2, data);
        }
        
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0L, 2L))
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
