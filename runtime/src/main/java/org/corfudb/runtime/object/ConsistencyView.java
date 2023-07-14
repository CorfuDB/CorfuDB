package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuOptions.ConsistencyModel;

public interface ConsistencyView {
    default ConsistencyModel getConsistencyModel() {
        return ConsistencyModel.READ_YOUR_WRITES;
    }
}
