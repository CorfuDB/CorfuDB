package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
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

    private final CorfuTablesGenerator tablesManager;

    public CheckpointOperation(State state, CorfuTablesGenerator tablesManager) {
        super(state, Operation.Type.CHECKPOINT);
        this.tablesManager = tablesManager;
    }

    @Override
    public void execute() {
        try {
            MultiCheckpointWriter<CorfuTable<String, String>> mcw = new MultiCheckpointWriter<>();
            mcw.addAllMaps(tablesManager.getMaps());
            Token trimAddress = mcw.appendCheckpoints(tablesManager.getRuntime(), "generator");

            TimeUnit.SECONDS.sleep(30);

            AddressSpaceView addressSpaceView = tablesManager.getRuntime().getAddressSpaceView();
            addressSpaceView.prefixTrim(trimAddress);
            addressSpaceView.gc();
            addressSpaceView.invalidateClientCache();
            addressSpaceView.invalidateServerCaches();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("Checkpoint doesn't contain data");
    }
}
