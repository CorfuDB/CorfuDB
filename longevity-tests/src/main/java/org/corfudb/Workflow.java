package org.corfudb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public abstract class Workflow {
    ScheduledExecutorService executor;
    Properties properties;
    AbstractIntegerDistribution distribution;
    double distributionMean;
    final static String BURST = "burst";
    final static String BASELINE = "baseline";
    final static int DEFAULT_NUM_ELEMENTS = 1000;
    final static double DEFAULT_EXPONENT = 1.2;
    boolean isRunning;

    public Workflow(String name, String propFilePath) {
        executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(name)
                        .build());
        try (InputStream input = Files.newInputStream(Paths.get(propFilePath))) {
            properties = new Properties();
            properties.load(input);
            int numElements = DEFAULT_NUM_ELEMENTS;
            double exponent = DEFAULT_EXPONENT;
            if (properties.getProperty("load.distribution") != null &&
                    properties.getProperty("load.distribution").equals("ZipfDistribution")) {
                numElements = Integer.parseInt(properties.getProperty("zipf.numElements"));
                exponent = Double.parseDouble(properties.getProperty("zipf.exponent"));
            }
            distribution = new ZipfDistribution(numElements, exponent);
            distributionMean = distribution.getNumericalMean();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    abstract void init(CorfuRuntime corfuRuntime, CommonUtils commonUtils);
    abstract void start();
    abstract void executeTask(long loadSize);
    public void submitTask(Duration interval) {
        isRunning = true;
        while (isRunning) {
            try {
                String loadType = BASELINE;
                long loadSize = distribution.sample();
                if (loadSize > distributionMean) {
                    loadType = BURST;
                }
                MicroMeterUtils.time(() -> executeTask(loadSize), "workflow.latency", "load", loadType);
                TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(interval.toMillis()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void stop() {
        this.executor.shutdownNow();
        isRunning = false;
    }

}


//Add bursts and baseline - use zipf distribution?
//end to end latency of workflow, notification receive timer -- Done
