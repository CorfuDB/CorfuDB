package org.corfudb.benchmark;

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
            CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
            parameters.setPrometheusMetricsPort(1234);
            CorfuRuntime runtime = CorfuRuntime.fromParameters(parameters);
            runtime.addLayoutServer(endPoint);
            runtime.connect();
            runtimes.add(runtime);
        }
    }

    protected CorfuRuntime getRuntime(int index) {
        return runtimes.get(index % numRuntimes);
    }
}