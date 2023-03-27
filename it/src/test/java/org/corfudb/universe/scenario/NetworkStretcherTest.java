package org.corfudb.universe.scenario;

import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;

// 3 Nodes are brought up, 1 node has all the data, let transfer finish, put one node in unresponsive list
// Transfer size: 10Mb, 100Mb
// Introduce Latencies: None, 500ms, 1000ms
// Network stretcher latencies: Default values, 5times faster
// Measure: Number of layout reconfigurations, Failure Detection Times
public class NetworkStretcherTest extends GenericIntegrationTest {
    @Test(timeout = 3000000)
    public void test() {
        workflow(wf -> {
            // wf.setupDocker(fix -> fix.getLogging().enabled(true));
            System.out.println("HELLO");
            wf.deploy();
            Sleep.sleepUninterruptibly(Duration.ofSeconds(10));
        });
    }
}
