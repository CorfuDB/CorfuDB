package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/3/18.
 */
public class OptimisticTxOperation extends Operation {
    public OptimisticTxOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        getConfiguration().getCorfuRuntime().getObjectsView().TXBegin();
        return null;
    }
}
