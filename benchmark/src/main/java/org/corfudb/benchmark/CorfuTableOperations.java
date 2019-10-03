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

import java.util.UUID;

/**
 * Operations for CorfuTable, contains build, ,put, get, put+get operations.
 */
@Slf4j
public class CorfuTableOperations extends Operation {
    private double ratio;
    CorfuTable<String, String> corfuTable;
    KeyValueManager keyValueManager;
    private Timer corfuTableBuildTimer;
    private Timer corfuTableGetTimer;
    private Timer corfuTablePutTimer;
    public static final double THRESHOULD = 50000.38;

    CorfuTableOperations(String name, CorfuRuntime runtime, CorfuTable corfuTable, int numRequest, double ratio, int keyNum, int valueSize) {
        super(runtime);
        shortName = name;
        this.corfuTable = corfuTable;
        this.numRequest = numRequest;
        this.ratio = ratio;
        keyValueManager = new KeyValueManager(keyNum, valueSize);
        fillTable();
        setMetrics();
    }

    private void fillTable() {
        for (int i = 0; i < 10; i ++) {
            String key = keyValueManager.generateKey();
            String value = keyValueManager.generateValue();
            corfuTable.put(key, value);
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

    private void putTable() {
        String key = keyValueManager.generateKey();
        String value = keyValueManager.generateValue();
        //System.out.println(value);
        try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
            corfuTable.put(key, value);
        }
    }

    private void getTable() {
        String key = keyValueManager.getKey();
        log.info("get value for key: " + key);
        try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
            String value = corfuTable.get(key);
            if (value == null) {
                log.info("not such key");
            } else {
                log.info("get value: "+value);
            }
        }
    }

    /**
     * Benchmark Tests for put operation.
     */
    private void corfuTablePut() {
        for (int i = 0; i < numRequest; i++) {
            putTable();
        }
    }

    /**
     * Benchmark Tests for get operation.
     */
    private void corfuTableGet() {
        for (int i = 0; i < numRequest; i++) {
            getTable();
        }
    }

    /**
     * Benchmark Operation for combination of put and get operations.
     */
    private void corfuTablePutGet() {
        int numPut = (int) (numRequest * ratio);
        int numGet = numRequest - numPut;
        int putI = 0;
        int getI = 0;
        while (putI < numPut && getI < numGet) {
            putTable();
            putI++;
            if (putI < numPut) {
                putTable();
                putI++;
            }
            getTable();
            getI++;
        }
        if (putI == numPut) {
            for (; getI < numGet; getI++) {
                getTable();
            }
        }
        for(; putI < numPut; putI++) {
            putTable();
        }
    }

    @Override
    public void execute() {
        switch (shortName) {
            case "build":
                corfuTableBuild();
                break;
            case "put":
                corfuTablePut();
                break;
            case "get":
                corfuTableGet();
                break;
            case "putget":
                corfuTablePutGet();
                break;
            default:
                log.error("no such operation for CorfuTable.");
        }
    }
}