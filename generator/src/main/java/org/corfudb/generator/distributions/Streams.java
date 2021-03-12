package org.corfudb.generator.distributions;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.generator.distributions.Streams.*;

/**
 * This class implements a distribution over the possible streams that
 * can be created/modified.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Streams implements DataSet<StreamName> {

    private final Set<StreamName> streamIds;
    private final int numStreams;

    public Streams(int num) {
        streamIds = new HashSet<>();
        numStreams = num;
    }

    @Override
    public void populate() {
        for (int tableId = 0; tableId < numStreams; tableId++) {
            streamIds.add(new StreamName(tableId));
        }
    }

    @Override
    public List<StreamName> getDataSet() {
        return new ArrayList<>(streamIds);
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    public static final class StreamName {
        private final int streamId;

        private String getTableName(){
            return "table_" + streamId;
        }

        public UUID getStreamId() {
            return CorfuRuntime.getStreamID(getTableName());
        }
    }
}
