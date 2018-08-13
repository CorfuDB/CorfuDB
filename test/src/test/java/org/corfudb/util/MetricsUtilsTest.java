package org.corfudb.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MetricsUtilsTest {
    @Rule
    public TemporaryFolder metricsFolder = new TemporaryFolder();

    @Test
    public void testMetricsCsvReport() throws Exception{
        System.setProperty("corfu.metrics.collection", Boolean.TRUE.toString());
        System.setProperty("corfu.metrics.csv.folder", metricsFolder.getRoot().getAbsolutePath());
        System.setProperty("corfu.metrics.csv.interval", Integer.valueOf(1).toString());

        MetricsUtils.metricsReportingSetup();
        MetricsUtils.metrics.counter("test_counter").inc(101);

        File testCounterCsv = Paths.get(metricsFolder.getRoot().getAbsolutePath(), "test_counter.csv").toFile();

        final int timeout = 3000;
        int waitTime = 0;
        while(!testCounterCsv.exists()){
            waitTime += 50;
            TimeUnit.MILLISECONDS.sleep(100);
            if(waitTime > timeout){
                fail("Wait for cvs report. The cvs report wasn't created");
            }
        }
        assertTrue(testCounterCsv.exists());
    }
}