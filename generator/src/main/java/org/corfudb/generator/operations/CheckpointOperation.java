package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.generator.State;
import org.corfudb.protocols.wireprotocol.LogicalSequenceNumber;
import org.corfudb.runtime.MultiCheckpointWriter;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class CheckpointOperation extends Operation {

    public CheckpointOperation(State state) {
        super(state);
        shortName = "Checkpoint";
    }

    @Override
    @SuppressWarnings("checkstyle:ThreadSleep")
    public void execute() {
        try {

//            TODO: uncomment when verification supports it
//            String cpStartRecord = String.format("%s, %s", shortName, "start");
//            Correctness.recordOperation(cpStartRecord, false);

            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addAllMaps(state.getMaps());
            LogicalSequenceNumber trimAddress = mcw.appendCheckpoints(state.getRuntime(), "Maithem");
            state.setTrimMark(trimAddress);
            Thread.sleep(1000l * 30l * 1l);
            LogicalSequenceNumber prefixTrimAddress = new LogicalSequenceNumber(trimAddress.getEpoch(), trimAddress.getSequenceNumber() - 1);
            state.getRuntime().getAddressSpaceView().prefixTrim(prefixTrimAddress);
            state.getRuntime().getAddressSpaceView().gc();
            state.getRuntime().getAddressSpaceView().invalidateClientCache();
            state.getRuntime().getAddressSpaceView().invalidateServerCaches();

//            TODO: uncomment when verification supports it
//            String cpStopRecord = String.format("%s, end, %s", shortName, trimAddress);
//            Correctness.recordOperation(cpStopRecord, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
