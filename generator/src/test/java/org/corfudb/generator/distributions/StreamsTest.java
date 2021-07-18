package org.corfudb.generator.distributions;

import org.corfudb.generator.distributions.Streams.StreamId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsTest {

    @Test
    public void testSample() {
        Streams streams = new Streams(1);
        streams.populate();
        StreamId sample = streams.sample();

        assertEquals(new StreamId(0), sample);
        assertEquals(sample, streams.sample());
    }

    @Test
    public void testStreamId() {
        Streams streams = new Streams(1);
        streams.populate();
        StreamId sample = streams.sample();

        assertEquals(UUID.nameUUIDFromBytes("0".getBytes()), sample.getStreamId());
    }
}