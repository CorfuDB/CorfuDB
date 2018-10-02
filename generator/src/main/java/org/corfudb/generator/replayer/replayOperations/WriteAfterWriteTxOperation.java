package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/12/18.
 */
public class WriteAfterWriteTxOperation extends Operation {
    public WriteAfterWriteTxOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        getConfiguration()
                .getCorfuRuntime()
                .getObjectsView()
                .TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
        return null;
    }
}
