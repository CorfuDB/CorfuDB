package org.corfudb.generator;

import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
public class RemoveOperation extends Operation {

    public RemoveOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        UUID streamID = (UUID) state.getStreams().sample(1).get(0);
        UUID key = (UUID) state.getKeys().sample(1).get(0);
        state.getMap(streamID).remove(key);
    }
}
