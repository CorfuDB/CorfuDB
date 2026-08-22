package org.corfudb.benchmark;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Defaults;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AuxReport implements InternalProfiler {
    public static double runResult = Double.NaN;

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {

    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
        List<ScalarResult> results = new ArrayList<>();
        results.add(new ScalarResult(Defaults.PREFIX + "aux.score",
                runResult,
                benchmarkParams.getTimeUnit().toString(), AggregationPolicy.SUM));
        return results;
    }

    @Override
    public String getDescription() {
        return "Long running TX";
    }
}
