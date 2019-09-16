package org.corfudb.benchmarks;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.StringIndexer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

@Slf4j
public class CorfuTableOperations extends Operation {
    //private Set<String> keySet;
    private boolean hasSecordaryIndex;
    private boolean cacheEnabled;
    CorfuTable<String, String> corfuTable;
    private Timer corfuTableBuildTimer;
    private Timer corfuTableGetTimer;
    private Timer corfuTablePutTimer;
    public static final double THRESHOULD = 50000.38;// different threshould for different operations

    CorfuTableOperations(String name, CorfuRuntime rt, int numRequest, boolean hasSecordaryIndex, boolean cacheEnabled) {
        super(rt);
        shortName = name;
        this.numRequest = numRequest;
        //this.numRemove = numRemove;
        this.hasSecordaryIndex = hasSecordaryIndex;
        this.cacheEnabled = cacheEnabled;
        fillTable();
        setTimer();
    }

    private void buildCorfuTable() {
        if (hasSecordaryIndex) {
            corfuTable = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setArguments(new StringIndexer())
                    .setStreamName("benchmark-corfutable")
                    .open();
        } else {
            corfuTable = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName("benchmark-corfutable")
                    .open();
        }
    }

    private void fillTable() {
        buildCorfuTable();
        for (int i = 0; i < numRequest; i ++) {
            corfuTable.put(String.valueOf(i), String.valueOf(i));
        }
    }

    void setTimer() {
        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        corfuTableBuildTimer = metricRegistry.timer(CorfuComponent.OBJECT +
                "corfutable-build");
        corfuTableGetTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-build-indexed");
        //corfuTableBuildCachedTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-build-cached");
        corfuTablePutTimer = metricRegistry.timer(CorfuComponent.OBJECT + "corfutable-put");

    }

    private void corfuTableBuild() {
        for (int i = 0; i < numRequest; i++) {
            if (hasSecordaryIndex) {
                try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableBuildTimer)) {
                    CorfuTable<String, String>
                            tempCorfuTable = rt.getObjectsView().build()
                            .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                            .setArguments(new StringIndexer())
                            .setStreamName("buildtest")
                            .open();
                }
            } else {
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
    }

    private void corfuTablePut() {
        for (int i = 0; i < numRequest; i++) {
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
                corfuTable.put(key, value);
            }
        }
    }

    private void corfuTableGet() {
        for (int i = 0; i < numRequest; i++) {
            String key = String.valueOf(i);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
                corfuTable.get(key);
            }
        }
    }

    private void runPutGet(double ratio) {
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
            String key = String.valueOf(i);
            String value = String.valueOf(i);
            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTableGetTimer)) {
                corfuTable.get(key);
            }
        }
    }
    private void corfuTablePutGet50() {
        runPutGet(0.5);
    }

    private void corfuTablePutGet20() {
        runPutGet(0.2);
    }

    private void corfuTablePutGet80 () {
        runPutGet(0.8);
    }

//    private void corfuTableRemove() {
//        for (int i = 0; i < numRemove; i++) {
//            String key = String.valueOf(i);
//            try (Timer.Context context = MetricsUtils.getConditionalContext(corfuTablePutTimer)) {
//                corfuTable.remove(key);
//            }
//        }
//    }

    @Override
    public void execute() {
        if (shortName.equals("build")) {
            corfuTableBuild();
        } else if (shortName.equals("put")) {
            corfuTablePut();
        } else if (shortName.equals("get")) {
            corfuTableGet();
        } else if (shortName.equals("putget50")) {
            corfuTablePutGet50();
        } else if (shortName.equals("putget20")) {
            corfuTablePutGet20();
        } else if (shortName.equals("putget80")) {
            corfuTablePutGet80();
        } else {
            log.error("no such operation for CorfuTable.");
        }
    }
}
