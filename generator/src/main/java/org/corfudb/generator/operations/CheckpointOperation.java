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
            long trimAddress = mcw.appendCheckpoints(state.getRuntime(), "Maithem");
            state.setTrimMark(trimAddress);
            Thread.sleep(1000 * 30 * 1);
            state.getRuntime().getAddressSpaceView().prefixTrim(trimAddress - 1);
            state.getRuntime().getAddressSpaceView().gc();
            state.getRuntime().getAddressSpaceView().invalidateClientCache();
            state.getRuntime().getAddressSpaceView().invalidateServerCaches();
            System.out.println("CheckpointOperation Completed");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
