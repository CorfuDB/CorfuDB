package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.AddressSpaceView;

import java.util.concurrent.TimeUnit;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class CheckpointOperation extends Operation {

    public CheckpointOperation(State state) {
        super(state, "Checkpoint");
    }

    @Override
    public void execute() {
        try {
            MultiCheckpointWriter<CorfuTable<String, String>> mcw = new MultiCheckpointWriter<>();
            mcw.addAllMaps(state.getMaps());
            Token trimAddress = mcw.appendCheckpoints(state.getRuntime(), "Maithem");
            state.updateTrimMark(trimAddress);
            TimeUnit.SECONDS.sleep(30);

            AddressSpaceView addressSpaceView = state.getRuntime().getAddressSpaceView();
            addressSpaceView.prefixTrim(trimAddress);
            addressSpaceView.gc();
            addressSpaceView.invalidateClientCache();
            addressSpaceView.invalidateServerCaches();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
