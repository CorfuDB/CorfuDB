package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

public class StreamIdTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBuildIllegalArguments() {
        StreamId.build(Optional.empty(), Optional.empty());
    }

    @Test
    public void testBuildValidArguments() {
        String streamName = "test";
        String id = "098f6bcd-4621-3373-8ade-4e832627b4f6";
        UUID uuid = CorfuRuntime.getStreamID(streamName);

        StreamId streamId = StreamId.build(Optional.of(streamName), Optional.empty());
        assertEquals(streamName, streamId.getName());
        assertEquals(UUID.fromString(id), streamId.getId());

        streamId = StreamId.build(Optional.empty(), Optional.of(uuid));
        assertEquals(Utils.toReadableId(uuid), streamId.getName());
        assertEquals(uuid, streamId.getId());

        streamId = StreamId.build(Optional.of(streamName), Optional.of(uuid));
        assertEquals(streamName, streamId.getName());
        assertEquals(uuid, streamId.getId());
    }
}
