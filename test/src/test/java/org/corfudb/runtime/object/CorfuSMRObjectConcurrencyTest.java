package org.corfudb.runtime.object;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 12/4/16.
 */
public class CorfuSMRObjectConcurrencyTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void testCorfuSharedCounterConcurrentReads() throws Exception {
        getDefaultRuntime();

/*
        TestClass sharedCounter = getRuntime().getObjectsView().
                build().
                setStreamName("test")
                .setType(TestClass.class)
                .open();
        sharedCounter.set(55);
*/

        CorfuSharedCounter sharedCounter = getRuntime().getObjectsView().
                build().
                setStreamName("test")
                .setType(CorfuSharedCounter.class)
                .open();
        sharedCounter.setValue(55);

        int concurrency = 10;
        int writeconcurrency = 5;
        int writerwork = 50000;

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
                    for (int i = 0; i < 1000; i++) {
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
        executeScheduled(concurrency, 50000, TimeUnit.MILLISECONDS);

    }
}
