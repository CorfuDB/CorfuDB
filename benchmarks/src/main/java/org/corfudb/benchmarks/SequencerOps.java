package org.corfudb.benchmarks;

import org.corfudb.runtime.CorfuRuntime;

public class SequencerOps extends Operation {
    SequencerOps(String name, CorfuRuntime rt) {
        super(rt);
        shortName = name;
    }

    @Override
    public void execute() {
        if (shortName.equals("query")) {
            rt.getSequencerView().query();
        }
    }
}
