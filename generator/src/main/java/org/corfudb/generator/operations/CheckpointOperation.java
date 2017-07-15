package org.corfudb.generator.operations;

import org.corfudb.generator.State;
import org.corfudb.runtime.MultiCheckpointWriter;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class CheckpointOperation extends Operation {

    public CheckpointOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        try {
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addAllMaps(state.getMaps());
            long trimAddress = mcw.appendCheckpoints(state.getRt(), "Maithem");
            Thread.sleep(1000 * 60 * 1);
            state.getRt().getAddressSpaceView().prefixTrim(trimAddress - 1);
            state.setTrimMark(trimAddress);
            state.getRt().getAddressSpaceView().gc();
            state.getRt().getAddressSpaceView().invalidateClientCache();
            state.getRt().getAddressSpaceView().invalidateServerCaches();
            log.info("CheckpointOperation Completed");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
