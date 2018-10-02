package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/16/18.
 */
public class AbortTxOperation extends Operation {
    public AbortTxOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        getConfiguration().getCorfuRuntime().getObjectsView().TXAbort();
        return null;
    }
}
