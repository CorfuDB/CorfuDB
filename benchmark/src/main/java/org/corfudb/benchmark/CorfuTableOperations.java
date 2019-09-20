package org.corfudb.benchmark;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

/**
 * Operations for CorfuTable, contains build, ,put, get, put+get operations.
 * TODO: add secondary index and cache, see performance difference, add memory size parameter.
 */
@Slf4j
public class CorfuTableOperations extends Operation {
    private double ratio;
    CorfuTable<String, String> corfuTable;
    private Timer corfuTableBuildTimer;
    private Timer corfuTableGetTimer;
    private Timer corfuTablePutTimer;
    //private Gauge<Long> corfuTableSizeGauge;
    public static final double THRESHOULD = 50000.38;

    CorfuTableOperations(String name, CorfuRuntime runtime, CorfuTable corfuTable, int numRequest, double ratio) {
        super(runtime);
        shortName = name;
        this.corfuTable = corfuTable;
        this.numRequest = numRequest;
        this.ratio = ratio;
        fillTable();
        setMetrics();
    }

    private void fillTable() {
        for (int i = 0; i < 10; i ++) {
            corfuTable.put(String.valueOf(i), String.valueOf(i));
        }
    }

    void setMetrics() {
        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        corfuTableBuildTimer = metricRegistry.timer(CorfuComponent.OBJECT +
                "corfutable-build");
        corfuTableGetTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-get");
        //corfuTableBuildCachedTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-build-cached");
        corfuTablePutTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-put");
        //corfuTableSizeGauge = MetricsUtils.addMemoryMeasurerFor(metricRegistry, corfuTable);
    }

    /**
     * Benchmark Tests for build operation.
     */
    private void corfuTableBuild() {
        for (int i = 0; i < numRequest; i++) {
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableBuildTimer)) {
                CorfuTable<String, String>
                        tempCorfuTable = rt.getObjectsView().build()
                        .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                        .setStreamName("buildtest")
                        .open();
            }
            if (corfuTableBuildTimer.getSnapshot().getMean() > 1.1 * THRESHOULD) {
                log.warn("CorfuTable build beyond normal performance time.");
            }
        }
    }

    /**
     * Benchmark Tests for put operation.
     */
    private void corfuTablePut() {
        for (int i = 0; i < numRequest; i++) {
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
                corfuTable.put(key, value);
            }
        }
    }

    /**
     * Benchmark Tests for get operation.
     */
    private void corfuTableGet() {
        for (int i = 0; i < numRequest; i++) {
            String key = String.valueOf(i % 10);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
                corfuTable.get(key);
            }
        }
    }

    /**
     * Benchmark Operation for combination of put and get operations.
     */
    private void corfuTablePutGet() {
        int numPut = (int) (numRequest * ratio);
        int numGet = numRequest - numPut;
        for (int i = 0; i < numPut; i++) {
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
                corfuTable.put(key, value);
            }
        }
        for (int i = 0; i < numGet; i++) {
            String key = String.valueOf(i % numPut);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
                corfuTable.get(key);
            }
        }
    }

    @Override
    public void execute() {
        if (shortName.equals("build")) {
            corfuTableBuild();
        } else if (shortName.equals("put")) {
            corfuTablePut();
        } else if (shortName.equals("get")) {
            corfuTableGet();
        } else if (shortName.equals("putget")) {
            corfuTablePutGet();
        } else {
            log.error("no such operation for CorfuTable.");
        }
    }
}
