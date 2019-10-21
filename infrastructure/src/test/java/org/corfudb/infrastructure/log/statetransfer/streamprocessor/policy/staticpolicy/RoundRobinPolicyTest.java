package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.math.LongRange;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RoundRobinPolicyTest {

    /**
     * If servers were provided, schedule in the round robin fashion.
     */
    @Test
    void testRoundRobinPolicy() {
        List<Long> collect = LongStream.range(0L, 15L).boxed().collect(Collectors.toList());
        Optional<List<String>> servers = Optional.of(ImmutableList.of("A", "B", "C"));
        int defaultSize = 5;
        StaticPolicyData data = new StaticPolicyData(collect, servers, defaultSize);

        RoundRobinPolicy rr = new RoundRobinPolicy();
        InitialBatchStream initialBatchStream = rr.applyPolicy(data);
        assertThat(initialBatchStream.getInitialBatchStreamSize()).isEqualTo(3);
        List<Long> collect1 = LongStream.range(0L, 5L).boxed().collect(Collectors.toList());
        List<Long> collect2 = LongStream.range(5L, 10L).boxed().collect(Collectors.toList());
        List<Long> collect3 = LongStream.range(10L, 15L).boxed().collect(Collectors.toList());
        List<Batch> expected = ImmutableList.of(
                new Batch(collect1, Optional.of("A")),
                new Batch(collect2, Optional.of("B")),
                new Batch(collect3, Optional.of("C")));
        assertThat(initialBatchStream.getInitialStream()).isEqualTo(expected);
    }

    /**
     * If servers were not provided schedule as is.
     */
    @Test
    void testScheduleAsIs(){
        List<Long> collect = LongStream.range(0L, 15L).boxed().collect(Collectors.toList());
        Optional<List<String>> servers = Optional.empty();
        int defaultSize = 5;
        StaticPolicyData data = new StaticPolicyData(collect, servers, defaultSize);

        RoundRobinPolicy rr = new RoundRobinPolicy();
        InitialBatchStream initialBatchStream = rr.applyPolicy(data);
        assertThat(initialBatchStream.getInitialBatchStreamSize()).isEqualTo(3);
        List<Long> collect1 = LongStream.range(0L, 5L).boxed().collect(Collectors.toList());
        List<Long> collect2 = LongStream.range(5L, 10L).boxed().collect(Collectors.toList());
        List<Long> collect3 = LongStream.range(10L, 15L).boxed().collect(Collectors.toList());
        List<Batch> expected = ImmutableList.of(
                new Batch(collect1, Optional.empty()),
                new Batch(collect2, Optional.empty()),
                new Batch(collect3, Optional.empty()));
        assertThat(initialBatchStream.getInitialStream()).isEqualTo(expected);


    }
}