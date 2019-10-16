
package org.corfudb.benchmark;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.*;

public class Streams {
    int numStreams;
    @Getter
    List<String> streams;

    Streams(int numStreams) {
        this.numStreams = numStreams;
        streams = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            streams.add("table_" + Integer.toString(i));
        }
    }

    public UUID getStreamID(int index) {
        return CorfuRuntime.getStreamID(streams.get(index % numStreams));
    }
    public String getStreamName(int index)  { return streams.get(index % numStreams);}

    public Map<UUID, String> getAllMapNames() {
        Map<UUID, String> allMapNames = new HashMap<> ();

        for(int i = 0; i < numStreams; i++) {
            allMapNames.put(getStreamID (i), getStreamName (i));
        }
        return  allMapNames;
    }
}