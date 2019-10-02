package org.corfudb.universe.scenario;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;


public class CorfuTableBenchmark{
    CorfuRuntime runtime;
    public CorfuTableBenchmark() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(1234);
        runtime = CorfuRuntime.fromParameters(parameters);
        runtime.addLayoutServer("localhost:9001");
        runtime.connect();

    }

    private void runTest() {
        CorfuTable<String, String> table1 = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("table1")
                .open();

        CorfuTable<String, String> table2 = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("table2")
                .open();

        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        TriFunction<Integer, CorfuTable<String, String>, Void> unitOfWork =
                (offset, table) -> {
                    Timer putTimer = metricRegistry.timer("corfutable-put");

                            StopWatch watch = new StopWatch();
                            watch.start();
                            for (int j = 0; j < 100000; j++) {
                                String key = "key-" + (j %10);
                                String value = RandomStringUtils.random(1700, true, true);
                                try (Timer.Context context = putTimer.time()) {
                                    table.put(key, value
                                            );
                                }  catch (TransactionAbortedException e) {
                                    e.printStackTrace();
                                }
                            }
                            watch.stop();
                            System.err.println(watch.getTime());
                    return null;
                };

        Thread t0 = new Thread(() -> unitOfWork.apply(0, table1));
        t0.start();

        Thread t1 = new Thread(() -> unitOfWork.apply(0, table2));
        t1.start();

//        Thread t2 = new Thread(() -> unitOfWork.apply(2, table1));
//        t2.start();
//
//        Thread t3 = new Thread(() -> unitOfWork.apply(5, table2));
//        t3.start();

        try {
            t0.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @FunctionalInterface
    interface TriFunction<T, V, R> {
        R apply(T a, V c);
    }

   public static void main(String[] args) {
        new CorfuTableBenchmark().runTest();
   }
}
