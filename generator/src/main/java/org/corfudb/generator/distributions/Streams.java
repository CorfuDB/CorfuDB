package org.corfudb.generator.distributions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class implements a distribution over the possible streams that
 * can be created/modified.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Streams implements DataSet<String> {

    private final Set<String> streamIds;
    private final int numStreams;

    public Streams(int num) {
        streamIds = new HashSet<>();
        numStreams = num;
    }

    @Override
    public void populate() {
        for (int tableId = 0; tableId < numStreams; tableId++) {
            streamIds.add("table_" + tableId);
        }
    }

    @Override
    public List<String> getDataSet() {
        return new ArrayList<>(streamIds);
    }
}
