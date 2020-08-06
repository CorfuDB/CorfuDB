package org.corfudb.integration;

import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This suite of tests validates the behavior of Log Replication
 * when nodes are shutdown and brought back up.
 *
 * @author amartinezman
 */
public class CorfuReplicationReconfigurationIT extends LogReplicationAbstractIT {

    private static final int SLEEP_DURATION = 5;

    /**
     * Sets the plugin path before starting any test
     *
     * @throws Exception
     */
    @Before
    public void setupPluginPath() {
        if(runProcess) {
            File f = new File(nettyConfig);
            this.pluginConfigFilePath = f.getAbsolutePath();
        } else {
            this.pluginConfigFilePath = nettyConfig;
        }
    }

    /**
     * Test the case where Standby leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testStandbyClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        System.out.println(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySync();

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Stop Standby Log Replicator Server
        System.out.println(">>> (2) Stop Standby Node");
        stopStandbyLogReplicator();

        // (3) Start daemon thread writing data to active
        System.out.println(">>> (3) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (4) Sleep Interval so writes keep going through, while standby is down
        System.out.println(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Standby Log Replicator
        System.out.println(">>> (5) Restart Standby Node");
        startStandbyLogReplicator();

        // (6) Verify Data on Standby after Restart
        System.out.println(">>> (6) Verify Data on Standby");
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }

    /**
     * Test the case where Active leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testActiveClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        System.out.println(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySync();

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Start daemon thread writing data to active
        System.out.println(">>> (2) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (3) Stop Standby Log Replicator Server
        System.out.println(">>> (3) Stop Active Node");
        stopActiveLogReplicator();

        // (4) Sleep Interval so writes keep going through, while standby is down
        System.out.println(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Active Log Replicator
        System.out.println(">>> (5) Restart Active Node");
        startActiveLogReplicator();

        // (6) Verify Data on Standby after Restart
        System.out.println(">>> (6) Verify Data on Standby");
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }
}
