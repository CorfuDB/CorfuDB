package org.corfudb.universe.scenario;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_LARGE_ITER;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.fail;

public class ReconcileIT extends GenericIntegrationTest {
    /**
     * Verify auto commit and state transfer after one node down and recover.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Stop one node
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by restarting the stopped node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void reconcileTest() {

        workflow(wf -> {
            wf.deploy();

            try {
                reconcile(wf);
            } catch (InterruptedException e) {
                fail("Test failed", e);
            }
        });
    }

    private void reconcile(UniverseManager.UniverseWorkflow<Fixture<UniverseParams>> wf) throws InterruptedException {
        String groupName = wf.getFixture().data().getGroupParamByIndex(0).getName();
        CorfuCluster corfuCluster = wf.getUniverse().getGroup(groupName);

        CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

        CorfuTable<String, String> table = corfuClient
                .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

        for (int i = 0; i < DEFAULT_LARGE_ITER; i++) {
            table.put(String.valueOf(i), String.valueOf(i));
        }

        CorfuRuntime runtime = corfuClient.getRuntime();
        runtime.getAddressSpaceView().commit(0L, DEFAULT_LARGE_ITER - 1);

        AtomicBoolean stopFlag = new AtomicBoolean(true);
        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            final int timeOut = 100;
            while (stopFlag.get()) {
                TokenResponse token = runtime.getSequencerView().next();
                runtime.getAddressSpaceView().write(token, "Test Payload".getBytes());
                Sleep.sleepUninterruptibly(Duration.ofMillis(timeOut));
            }
        });

        //Should stop one node and then restart
        CorfuServer server0 = corfuCluster.getFirstServer();

        // Stop one node and wait for layout's unresponsive servers to change
        server0.stop(Duration.ofSeconds(10));
        waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

        // Verify layout, unresponsive servers should contain only the stopped node
        Layout layout = corfuClient.getLayout();
        assertThat(layout.getUnresponsiveServers()).containsExactly(server0.getEndpoint());

        // Verify cluster status is DEGRADED with one node down
        ClusterStatusReport clusterStatusReport = corfuClient
                .getManagementView()
                .getClusterStatus();
        assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatusReport.ClusterStatus.DEGRADED);

        Map<String, ClusterStatusReport.NodeStatus> statusMap = clusterStatusReport.getClusterNodeStatusMap();
        assertThat(statusMap.get(server0.getEndpoint())).isEqualTo(ClusterStatusReport.NodeStatus.DOWN);

        // Verify data path working fine
        for (int i = 0; i < DEFAULT_LARGE_ITER; i++) {
            assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
        }

        // restart the stopped node and wait for layout's unresponsive servers to change
        server0.start();
        waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

        final Duration sleepDuration = Duration.ofSeconds(1);
        // Verify cluster status is STABLE
        clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
        while (!clusterStatusReport.getClusterStatus().equals(ClusterStatusReport.ClusterStatus.STABLE)) {
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            TimeUnit.MILLISECONDS.sleep(sleepDuration.toMillis());
        }
        assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatusReport.ClusterStatus.STABLE);

        stopFlag.set(false);
        writerFuture.complete(null);

        // Verify data path working fine
        for (int i = 0; i < DEFAULT_LARGE_ITER; i++) {
            assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
        }

        // Verify committed part.
        long committedTail = runtime.getAddressSpaceView().getCommittedTail();
        assertThat(committedTail).isNotEqualTo(DEFAULT_LARGE_ITER - 1);
        List<String> endpoints = corfuClient.getLayout().getLayoutServers();
        for (String endpoint : endpoints) {
            assertThat(getEmptyDataInRange(runtime, endpoint, committedTail)).isEmpty();
        }
    }

    private List<Long> getRangeAddressAsList(long startAddress, long endAddress) {
        Range<Long> range = Range.closed(startAddress, endAddress);
        return ContiguousSet.create(range, DiscreteDomain.longs()).asList();
    }

    private List<LogData> getEmptyDataInRange(CorfuRuntime corfuRuntime,
                                              String endpoint, long end) throws InterruptedException {
        final int maxRetries = 3;

        for (int i = 1; i <= maxRetries; i++) {
            try {
                ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                        .getLogUnitClient(endpoint)
                        .readAll(getRangeAddressAsList(0L, end))
                        .get();

                return readResponse.getAddresses().values()
                        .stream()
                        .filter(LogData::isEmpty)
                        .collect(Collectors.toList());
            } catch (ExecutionException e) {
                //
            }
        }

        return ImmutableList.of(LogData.getEmpty(0L));
    }
}
