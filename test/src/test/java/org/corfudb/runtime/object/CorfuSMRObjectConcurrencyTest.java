package org.corfudb.runtime.object;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 12/4/16.
 */
public class CorfuSMRObjectConcurrencyTest extends AbstractObjectTest {
    @Test
    public void testCorfuSharedCounterConcurrentReads() throws Exception {
        getDefaultRuntime();

        final int COUNTER_INITIAL = 55;

        CorfuSharedCounter sharedCounter = (CorfuSharedCounter)
                instantiateCorfuObject(CorfuSharedCounter.class, "test");

        sharedCounter.setValue(COUNTER_INITIAL);

        int concurrency = PARAMETERS.CONCURRENCY_SOME * 2;
        int writeconcurrency = PARAMETERS.CONCURRENCY_SOME;
        int writerwork = PARAMETERS.NUM_ITERATIONS_LOW;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduleConcurrently(writeconcurrency, t -> {
                    for (int i = 0; i < writerwork; i++)
                        sharedCounter.setValue(t*writerwork + i);
                }
        );
        scheduleConcurrently(concurrency-writeconcurrency, t -> {
                    int lastread = -1;
                    for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                        int res = sharedCounter.getValue();
                        boolean assertflag =
                                (
                                        ( ((lastread < writerwork && res < writerwork) || (lastread >= writerwork && res >= writerwork) ) && lastread <= res ) ||
                                                ( (lastread < writerwork && res >= writerwork) || (lastread >= writerwork && res < writerwork) )
                                );
                        assertThat(assertflag)
                                .isTrue();
                    }
                }
        );
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_LONG);

    }
}
