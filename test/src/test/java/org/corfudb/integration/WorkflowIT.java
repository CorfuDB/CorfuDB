package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.Harness.run;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.util.Simulator;
import org.corfudb.util.SimulatorConfig;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * This integration test verifies the behaviour of the add node workflow. In particular, a single node
 * cluster is created and then populated with data, then a new node is added to the cluster,
 * making it of size 2. Checkpointing is then triggered so that the new second node starts servicing
 * new writes, then a 3rd node is added. The third node will have the checkpoints of the CorfuTable
 * that were populated and checkpointed when the cluster was only 2 nodes. Finally, a client reads
 * back the data generated while growing the cluster to verify that it is correct and can be read
 * from a three node cluster.
 * <p>
 * Created by Maithem on 12/1/17.
 */
@Slf4j
public class WorkflowIT {

    @Test
    public void addAndRemoveNodeIT() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        SimulatorConfig config = SimulatorConfig
                .builder()
                .numThreads(250)
                .numTables(300)
                .burstPeriod(10 * 1000)
                .maxTxnPerPeriod(7000)
                .minTxnPerPeriod(1000)
                .stdTxnPerPeriod(3000)
                .medianTxnPerPeriod(3000)
                .maxWriteSize(16777216)
                .minWriteSize(1000)
                .stdWriteSize(14000)
                .medianWriteSize(2000)
                .maxTablesPerTxn(50)
                .minTablesPerTxn(1)
                .stdTablesPerTxn(5)
                .medianTablesPerTxn(5)
                .maxKeysPerTxn(100)
                .minKeysPerTxn(1)
                .stdKeysPerTxn(5)
                .medianKeysPerTxn(8)
                .maxKeysPerTxn(1000)
                .minKeySize(100)
                .medianKeySize(100)
                .stdKeySize(100)
                .build();

        Simulator sim = new Simulator(rt, config);
        sim.start();

        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60 * 1000);
                    long globalTs = rt.getSequencerView().next().getTokenValue();
                    rt.getAddressSpaceView().prefixTrim(Math.max(globalTs - 10, 0));
                    rt.getAddressSpaceView().gc();
                    log.info("Prefix Trim address {}", globalTs);
                } catch (Exception e) {
                    log.error("Error while trimming", e);
                    return;
                }
            }
        });
        thread.start();

        thread.join();
        Thread.sleep(1000 * 60 * 3);
    }
}