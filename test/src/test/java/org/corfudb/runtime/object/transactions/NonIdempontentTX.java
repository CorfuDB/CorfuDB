package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 1/6/17.
 */
public class NonIdempontentTX extends AbstractTransactionContextTest {

    /**
     * This test is currently failing, hence I disabled it.
     * Left as a fix, todo!
     */
    @Test
    public void staggeredNonidempotentTXs() {

        CorfuRuntime runtime1 = getRuntime();

        CorfuSharedCounter ctr = instantiateCorfuObject(
                CorfuSharedCounter.class, "nonidepmpotenttestcntr"
        );
        ctr.setValue(0);

        runtime1.getObjectsView().TXBegin();
        ctr.increment();

        // create a separate runtime,
        // and commit in it transactions on the same counter
        CorfuRuntime runtime2 = getDefaultRuntime();
        CorfuSharedCounter ctr2 = runtime2.getObjectsView()
                .build()
                .setStreamName("nonidempotenttestcntr")     // stream name
                .setType(CorfuSharedCounter.class)          // object class backed by this stream
                .open();                                   // instantiate the object!

        final int nTXs = 5;
        for (int t = 0; t < nTXs; t++) {
            ctr2.increment();
        }

        // now, commit the first TX on the first runtime
        runtime1.getObjectsView().TXEnd();

        assertThat(ctr.getValue())
                .isEqualTo(nTXs+1);
    }
}
