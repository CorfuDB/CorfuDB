package org.corfudb.runtime.view;

import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.protocols.sequencers.NettyStreamingSequencerProtocol;
import org.corfudb.util.RandomOpenPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 9/16/15.
 */
public class NewStreamingSequencerTest {

    LocalCorfuDBInstance instance;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Before
    public void setup()
            throws Exception
    {
        instance = CorfuDBRuntimeIT.generateInstance();
    }

    @Test
    public void sequenceNumbersAreUnique()
    {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        final int NUM_REQUESTS = 1000;
        for (int i = 0; i < NUM_REQUESTS; i++)
        {
            futures.add(instance.getNewStreamingSequencer().nextTokenAsync(UUID.randomUUID(), 1));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        Set<Long> allItems = new HashSet<>();
        Set<Long> duplicates =
        futures.stream()
                .map(CompletableFuture::join)
                .filter(n -> !allItems.add(n))
                .collect(Collectors.toSet());

        assertThat(duplicates.size())
                .isEqualTo(0);

        assertThat(allItems.size())
                .isEqualTo(NUM_REQUESTS);
    }

    @Test
    public void sequenceNumbersAreIncreasing()
            throws Exception
    {
        instance.getNewStreamingSequencer().nextTokenAsync(UUID.randomUUID(), 1)
                .thenApplyAsync((Long l) -> {
                            Long next = l;
                            try {
                                next = instance.getNewStreamingSequencer().nextTokenAsync(UUID.randomUUID(), 1).get();
                            } catch (Exception e) {
                                assertThat(e)
                                        .isNull();
                            }
                            assertThat(next)
                                    .isGreaterThan(l);
                            return l;
                        }
                ).thenApplyAsync((Long l) -> {
                    Long next = l;
                    try {
                        next = instance.getNewStreamingSequencer().nextTokenAsync(UUID.randomUUID(), 1).get();
                    } catch (Exception e) {
                        assertThat(e)
                                .isNull();
                    }
                    assertThat(next)
                            .isGreaterThan(l);
                    return l;
                }).join();
    }
}
