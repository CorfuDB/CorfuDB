package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.generator.State;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;

@Slf4j
public class InsertHoleOperation extends Operation {

    public InsertHoleOperation(State state) {
        super(state);
        shortName = "Hole";
    }

    @Override
    public void execute() {
        String streamId = (String) state.getStreams().sample(1).get(0);
        Token tk = state.getRuntime().getSequencerView().next(CorfuRuntime.getStreamID(streamId)).getToken();
        state.getRuntime().getLayoutView().getRuntimeLayout().getLogUnitClient(state.getRuntime().getLayoutServers().get(0)).fillHole(tk);
        log.info("Insert hole for stream {} at address {}", streamId, tk);
    }
}
