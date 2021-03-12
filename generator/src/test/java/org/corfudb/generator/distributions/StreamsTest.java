package org.corfudb.generator.distributions;

import org.corfudb.generator.distributions.Streams.StreamName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class StreamsTest {

    @Test
    public void testSample() {
        Streams streams = new Streams(1);
        streams.populate();
        StreamName sample = streams.sample();

        assertEquals(new StreamName(0), sample);
        assertEquals(sample, streams.sample());
    }

    @Test
    public void testStreamId() {
        Streams streams = new Streams(1);
        StreamName sample = streams.sample();

        assertEquals(UUID.nameUUIDFromBytes("table_0".getBytes()), sample.getStreamId());
    }
}