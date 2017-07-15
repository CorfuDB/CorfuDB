package org.corfudb.generator.operations;

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {

    public RemoveOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        UUID streamID = (UUID) state.getStreams().sample(1).get(0);
        String key = (String) state.getKeys().sample(1).get(0);
        state.getMap(streamID).remove(key);
    }
}
