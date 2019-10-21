package org.corfudb.common.streamutils;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.streamutils.StreamUtils.StreamHeadAndTail;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class StreamUtilsTest {

    @Test
    void testSplitStream() {
        // Several elements.
        Stream<Long> stream = LongStream.range(0L, 5L).boxed();
        StreamHeadAndTail<Long> longStreamHeadAndTail = StreamUtils.splitStream(stream, 5);
        Optional<Long> head = longStreamHeadAndTail.getHead();
        assertThat(head).isPresent();
        assertThat(head.get()).isEqualTo(0L);
        Stream<Optional<Long>> tail = longStreamHeadAndTail.getTail();
        List<Optional<Long>> collectedTail = tail.collect(Collectors.toList());
        assertThat(collectedTail)
                .isEqualTo(LongStream
                        .range(1L, 5L)
                        .boxed()
                        .map(Optional::of)
                        .collect(Collectors.toList()));

        // One element.
        Stream<Long> oneElementStream = Collections.singletonList(1L).stream();
        StreamHeadAndTail<Long> oneElementHeadAndTail = StreamUtils.splitStream(oneElementStream, 1);
        Optional<Long> head2 = oneElementHeadAndTail.getHead();
        assertThat(head2).isPresent();
        assertThat(head2.get()).isEqualTo(1L);
        List<Optional<Long>> emptyTail = oneElementHeadAndTail.getTail().collect(Collectors.toList());
        assertThat(emptyTail).isEmpty();
        // No elements.
        List<Long> emptyList = ImmutableList.of();
        Stream<Long> emptyStream = emptyList.stream();
        StreamHeadAndTail<Long> emtpyHeadAndTail = StreamUtils.splitStream(emptyStream, 0);
        Optional<Long> emptyHead = emtpyHeadAndTail.getHead();
        Stream<Optional<Long>> emptyTail2 = emtpyHeadAndTail.getTail();
        assertThat(emptyHead).isNotPresent();
        assertThat(emptyTail2).isEmpty();

    }

}