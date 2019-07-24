package org.corfudb.integration;

import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogSizeQuotaIT extends AbstractIT {

    final int payloadSize = 1000;

    private Process runServerWithQuota(int port, long quota, boolean singleNode) throws Exception {
        String logPath = getCorfuServerLogPath(DEFAULT_HOST, port);
        FileStore corfuDirBackend = Files.getFileStore(Paths.get(CORFU_LOG_PATH));
        long fsSize = corfuDirBackend.getTotalSpace();
        final double HUNDRED = 100.0;
        final double quotaInPerc = quota * HUNDRED / fsSize;
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setSingle(singleNode)
                .setLogPath(logPath)
                .setLogSizeLimitPercentage(Double.toString(quotaInPerc))
                .runServer();
    }

    private void exhaustQuota(IStreamView sv) {
        final byte[] payload = new byte[payloadSize];
        while (true) {
            try {
                sv.append(payload);
            } catch (QuotaExceededException eq) {
                break;
            }
        }
    }

    @Test
    public void testLogUnitQuota() throws Exception {
        // This test does the following:
        // 0. Set a quota on the logunit
        // 1. Write to a map
        // 2. Exhaust the quota
        // 3. Verify that write operations start failing after the quota is exhausted
        // 4. Create a high priority client and verify that write requests go through (i.e. bypass quota checks)
        // 5. Run a checkpoint/trim cycle to free up some space
        // 6. Once there is available quota, make sure that write requests from regular clients will be accepted

        long maxLogSize = FileUtils.ONE_MB / 2;
        Process server_1 = runServerWithQuota(DEFAULT_PORT, maxLogSize, true);


        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT).connect();

        Map<String, String> map = rt.getObjectsView()
                .build()
                .setStreamName("s1")
                .setType(CorfuTable.class)
                .open();

        // Create a map
        map.put("k1", "v1");
        map.put("k2", "v2");

        // Create a stream and keep writing data till the quota is exhausted
        // quota is exhausted
        UUID streamId = UUID.randomUUID();
        IStreamView sv = rt.getStreamsView().get(streamId);

        exhaustQuota(sv);

        // Verify that transactions will fail after the quota is exceeded
        boolean txnAborted = false;
        try {
            rt.getObjectsView().TXBegin();
            map.put("largeEntry", "Val");
            rt.getObjectsView().TXEnd();
        } catch (TransactionAbortedException tae) {
            assertThat(tae.getAbortCause()).isEqualTo(AbortCause.QUOTA_EXCEEDED);
            txnAborted = true;
        }

        assertThat(txnAborted).isTrue();

        // bump up the sequencer counter to create multiple empty segments
        final int emptySlots = StreamLogFiles.RECORDS_PER_LOG_FILE;
        for (int x = 0; x < emptySlots; x++) {
            rt.getSequencerView().next();
        }

        // Create a privileged client to checkpoint and compact
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .priorityLevel(PriorityLevel.HIGH)
                .build();

        CorfuRuntime privilegedRt = CorfuRuntime.fromParameters(params);
        privilegedRt.parseConfigurationString(DEFAULT_ENDPOINT);
        privilegedRt.connect();


        Map<String, String> map2 = privilegedRt.getObjectsView()
                .build()
                .setStreamName("s1")
                .setType(CorfuTable.class)
                .open();

        // Verify that a high priority client can write to a map
        map2.put("k4", "v4");

        // Verify that the high priority client can checkpoint/trim
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map2);
        Token token = mcw.appendCheckpoints(privilegedRt, "privilegedWriter");
        privilegedRt.getAddressSpaceView().prefixTrim(token);
        privilegedRt.getAddressSpaceView().gc();

        // Now verify that the original client (i.e. has a normal priority) is
        // able to write after some of the quota has been freed
        map.put("k3", "v3");


        // Now verify that all those changes can be observed from a new client
        CorfuRuntime rt3 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        Map<String, String> map3 = rt3.getObjectsView()
                .build()
                .setStreamName("s1")
                .setType(CorfuTable.class)
                .open();

        assertThat(map3.get("k1")).isEqualTo("v1");
        assertThat(map3.get("k2")).isEqualTo("v2");
        assertThat(map3.get("k3")).isEqualTo("v3");
        assertThat(map3.get("k4")).isEqualTo("v4");
    }

    @Test
    public void restartTest() throws Exception {
        // Verify that quota checks don't prevent the server from restarting
        long maxLogSize = FileUtils.ONE_MB / 2;
        Process server_1 = runServerWithQuota(DEFAULT_PORT, maxLogSize, true);

        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        UUID streamId = UUID.randomUUID();
        IStreamView sv = rt.getStreamsView().get(streamId);
        exhaustQuota(sv);

        // Restart the server and try to write again
        shutdownCorfuServer(server_1);
        server_1 = runServerWithQuota(DEFAULT_PORT, maxLogSize, true);

        assertThatThrownBy(() -> sv.append(new byte[payloadSize]))
                .isInstanceOf(QuotaExceededException.class);
    }

    @Test
    public void clusteringTest() throws Exception {

        long maxLogSize = FileUtils.ONE_MB / 2;
        int n1Port = DEFAULT_PORT;
        int n2Port = DEFAULT_PORT + 1;
        int n3Port = DEFAULT_PORT + 2;
        Process server_1 = runServerWithQuota(n1Port, maxLogSize, true);
        Process server_2 = runServerWithQuota(n2Port, maxLogSize, false);
        Process server_3 = runServerWithQuota(n3Port, maxLogSize, false);

        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        IStreamView sv = rt.getStreamsView().get(UUID.randomUUID());
        exhaustQuota(sv);

        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofMillis(50);
        final int workflowNumRetry = 3;

        rt.getManagementView().addNode(DEFAULT_HOST + ":" + n2Port, workflowNumRetry,
                timeout, pollPeriod);
        rt.getManagementView().addNode(DEFAULT_HOST + ":" + n3Port, workflowNumRetry,
                timeout, pollPeriod);

        final int clusterSizeN3 = 3;
        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN3
                && layout.getSegments().size() == 1, rt);

        // Verify that we can't write to the cluster
        assertThatThrownBy(() -> sv.append(new byte[payloadSize]))
                .isInstanceOf(QuotaExceededException.class);

        // Verify that high priority clients can still write to the cluster
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .priorityLevel(PriorityLevel.HIGH)
                .build();

        CorfuRuntime privilegedRt = CorfuRuntime.fromParameters(params);
        privilegedRt.parseConfigurationString(DEFAULT_ENDPOINT);
        privilegedRt.connect();

        IStreamView sv2 = privilegedRt.getStreamsView().get(UUID.randomUUID());
        long currTail = privilegedRt.getSequencerView().query().getSequence();
        assertThat(sv2.append(new byte[payloadSize])).isEqualTo(currTail + 1);

        // Force cluster reconfiguration and make sure that state transfer is able
        // progress despite the quota restriction
        shutdownCorfuServer(server_2);
        waitForLayoutChange(layout -> layout.getActiveLayoutServers().size() == 2, rt);
        runServerWithQuota(n2Port, maxLogSize, false);
        waitForLayoutChange(layout ->  layout.getSegments().size() == 1 &&
                layout.getActiveLayoutServers().size() == clusterSizeN3, rt);
    }
}