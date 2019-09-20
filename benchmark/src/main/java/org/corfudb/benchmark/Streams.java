package org.corfudb.benchmark;

import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Streams {
    int numStreams;
    List<String> streams;

    Streams(int numStreams) {
        this.numStreams = numStreams;
        streams = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            streams.add("table_" + Integer.toString(i));
        }
    }

    protected UUID getStreamID(int index) {
        return CorfuRuntime.getStreamID(streams.get(index % numStreams));
    }
}
