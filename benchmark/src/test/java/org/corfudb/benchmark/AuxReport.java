package org.corfudb.benchmark;

import com.google.common.collect.FluentIterable;
import org.apache.commons.math3.stat.StatUtils;
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
    public static List<Double> runResultList0 = new ArrayList<>();
    public static List<Double> runResultList1 = new ArrayList<>();

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {

    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
        List<ScalarResult> results = new ArrayList<>();
        if (runResultList0.isEmpty()) {
            results.add(new ScalarResult(Defaults.PREFIX + "aux.score",
                    runResult,
                    benchmarkParams.getTimeUnit().toString(), AggregationPolicy.SUM));
        } else {
            results.add(new ScalarResult(Defaults.PREFIX + "aux0.score",
                    StatUtils.mean(runResultList0.stream().mapToDouble(Double::doubleValue).toArray()),
                    benchmarkParams.getTimeUnit().toString(), AggregationPolicy.SUM));
            runResultList0.clear();
            results.add(new ScalarResult(Defaults.PREFIX + "aux1.score",
                    StatUtils.mean(runResultList1.stream().mapToDouble(Double::doubleValue).toArray()),
                    benchmarkParams.getTimeUnit().toString(), AggregationPolicy.SUM));
            runResultList1.clear();
        }
        return results;
    }

    @Override
    public String getDescription() {
        return "Long running TX";
    }
}
