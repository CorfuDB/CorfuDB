package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/3/18.
 */
public class CommitTxOperation extends Operation {
    public CommitTxOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        return getConfiguration().getCorfuRuntime().getObjectsView().TXEnd();
    }
}
