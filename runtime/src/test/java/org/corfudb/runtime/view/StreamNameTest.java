package org.corfudb.runtime.view;

import org.corfudb.runtime.view.StreamsView.StreamId;
import org.corfudb.runtime.view.StreamsView.StreamName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class StreamNameTest {

    public static final String STREAM_NAME = "test_stream";
    public static final String STREAM_ID_STR = "65d3dade-8e78-345d-a38f-9ed776c4f545";

    @Test
    public void testName() {
        var name = StreamName.build(STREAM_NAME);
        assertEquals(STREAM_ID_STR, name.getId().getId().toString());
    }

    @Test
    public void testBuilder() {
        var name = StreamName.builder()
                .id(StreamId.build(STREAM_NAME))
                .name(Optional.of(STREAM_NAME))
                .build();
        assertEquals(STREAM_ID_STR, name.getId().getId().toString());
    }

    @Test
    public void testInvalidName() {
        Executable testCase = () -> StreamName.builder()
                .id(StreamId.build(STREAM_NAME))
                .name(Optional.of("invalid"))
                .build();
        assertThrows(java.lang.IllegalArgumentException.class, testCase);
    }
}