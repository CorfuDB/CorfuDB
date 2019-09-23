package org.corfudb.benchmark;

import org.corfudb.generator.distributions.DataSet;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;

public class Runtimes {
    int numRuntimes;
    String endPoint;
    List<CorfuRuntime> runtimes;

    Runtimes(int numRuntimes, String endPoint) {
        this.numRuntimes = numRuntimes;
        this.endPoint = endPoint;
        runtimes = new ArrayList<>();
        for (int i = 0; i < numRuntimes; i++) {
            runtimes.add(new CorfuRuntime(endPoint).connect());
        }
    }

    protected CorfuRuntime getRuntime(int index) {
        return runtimes.get(index % numRuntimes);
    }
}