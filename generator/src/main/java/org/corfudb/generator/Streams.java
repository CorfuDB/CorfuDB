package org.corfudb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
public class Streams extends DataSet {

    final private Set<UUID> streams;
    final private int numStreams;

    public Streams(int num) {
        streams = new HashSet<>();
        numStreams = num;
    }

    @Override
    public void populate() {
        for (int x = 0; x < numStreams; x++) {
            streams.add(UUID.randomUUID());
        }
    }

    @Override
    public List<UUID> getDataSet() {
        return new ArrayList<>(streams);
    }
}
