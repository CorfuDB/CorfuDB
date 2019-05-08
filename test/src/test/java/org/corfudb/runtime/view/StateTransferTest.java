package org.corfudb.runtime.view;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import lombok.Getter;
import org.corfudb.common.compression.Codec;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatusReliability;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus.STABLE;
import static org.corfudb.test.TestUtils.setAggressiveTimeouts;
import static org.corfudb.test.TestUtils.waitForLayoutChange;

/**
 * Created by zlokhandwala on 2019-06-06.
 */
public class StateTransferTest extends AbstractViewTest {

    @Getter
    protected CorfuRuntime corfuRuntime = null;

    @Before
    public void clearRuntime() {
        corfuRuntime = null;
    }

    @After
    public void shutdownRuntime() {
        if (corfuRuntime != null && !corfuRuntime.isShutdown()) {
            corfuRuntime.shutdown();
        }
    }

    private Map<Long, LogData> getAllNonEmptyData(CorfuRuntime corfuRuntime,
                                                  String endpoint, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .readAll(ContiguousSet.create(Range.closed(0L, end), DiscreteDomain.longs()).asList())
                .get();
        return readResponse.getAddresses().entrySet()
                .stream()
                .filter(longLogDataEntry -> !longLogDataEntry.getValue().isEmpty())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }


    @Test
    /**
     * 1. Segment 1: 0 -> -1: Node 0, Node 1
     * 2. Add node Node 2
     */
    public void verifyAddNode() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(-1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l1);
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();


        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        addServer(SERVERS.PORT_2);
        final int addNodeRetries = 3;
        rt.getManagementView()
                .addNode(SERVERS.ENDPOINT_2, addNodeRetries, Duration.ofMinutes(1L), Duration.ofSeconds(1));

        rt.invalidateLayout();
        final long expectedEpoch = 3L;

        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(expectedEpoch)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(rt.getLayoutView().getLayout()).isEqualTo(expectedLayout);
        long lastAddress = rt.getSequencerView().query(CorfuRuntime.getStreamID("test"));
        Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);

        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
    }

    @Test
    /**
     * 1. Segment 1: 0 -> 3: Node 0, Node 1
     * 2. Segment 2: 3 -> infinity: Node 0, Node 1, Node 2
     * 2. Node 2 eventually restores itself to the layout and merges segments
     */
    public void verifyRedundancyRestoration() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        final long writtenAddressesBatch1 = 3L;
        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l1);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        ClusterStatusReport clusterStatus = null;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            clusterStatus = rt.getManagementView().getClusterStatus();
            if (clusterStatus.getClusterStatus().equals(ClusterStatus.DB_SYNCING)) {
                Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
            } else {
                break;
            }
        }

        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(clusterStatus.getClusterStatus()).isEqualTo(STABLE);
        rt.invalidateLayout();
        assertThat(rt.getLayoutView().getLayout()).isEqualTo(expectedLayout);
        long lastAddress = rt.getSequencerView().query(CorfuRuntime.getStreamID("test"));
        Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);

        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());

    }


    /**
     * The test first creates a layout with 2 segments.
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> infinity (exclusive) Node 0, Node 1
     * Now a new node, Node 2 is added to the layout which splits the last segment, replicates
     * and merges the previous segment to produce the following:
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> infinity (exclusive) Node 0, Node 1, Node 2
     * Finally the state transfer is verified by asserting Node 1's data with Node 2's data.
     */
    @Test
    public void verifyStateTransferAndMerge() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        final long writtenAddressesBatch1 = 3L;
        final long writtenAddressesBatch2 = 6L;
        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l1);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        // Write to address spaces 0 to 2 (inclusive) to SERVER 0 only.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Write to address spaces 3 to 5 (inclusive) to SERVER 0 and SERVER 1.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        addServer(SERVERS.PORT_2);
        final int addNodeRetries = 3;
        rt.getManagementView()
                .addNode(SERVERS.ENDPOINT_2, addNodeRetries, Duration.ofMinutes(1L), Duration.ofSeconds(1));

        rt.invalidateLayout();
        final long finalEpochAfterAdd = 3L;

        Layout expectedLayout1 = new TestLayoutBuilder()
                .setEpoch(finalEpochAfterAdd)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(writtenAddressesBatch2)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch2)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        ClusterStatusReport clusterStatus = rt.getManagementView().getClusterStatus();
        Map<String, ConnectivityStatus> nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        Map<String, NodeStatus> nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        ClusterStatusReliability clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.DB_SYNCING);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.UP);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.DB_SYNCING);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReliability.STRONG_QUORUM);
        // If both of the segments were restored within the same epoch change concurrently, epoch's 3,
        // otherwise its 4.
        assertThat(rt.getLayoutView().getLayout().equals(expectedLayout1)
                || rt.getLayoutView().getLayout()
                .equals(new LayoutBuilder(expectedLayout1).setEpoch(finalEpochAfterAdd + 1).build())).isTrue();

        long lastAddress = rt.getSequencerView().query(CorfuRuntime.getStreamID("test"));

        Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);

        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
    }

    /**
     * The test first creates a layout with 3 segments.
     * Initial layout:
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> 6 (exclusive) Node 0, Node 1
     * Segment 3: 6 -> infinity (exclusive) Node 0, Node 1
     * <p>
     * Now a failed node, Node 2 is healed back which results in the following intermediary
     * states.
     * <p>
     * First, last segment will be split.
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> 6 (exclusive) Node 0, Node 1
     * Segment 3: 6 -> 9 (exclusive) Node 0, Node 1
     * Segment 4: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     * <p>
     * Then, healing carries out a cleaning task of merging the segments.
     * (transfer segment 1 to Node 1 and merge segments 1 and 2.)
     * Segment 1: 0 -> 6 (exclusive) Node 0, Node 1
     * Segment 2: 6 -> 9 (exclusive) Node 0, Node 1
     * Segment 3: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     * <p>
     * And then:
     * Segment 1: 0 -> 9 (exclusive) Node 0, Node 1
     * Segment 2: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     * <p>
     * At the end, the stable layout will be the following:
     * Segment 1: 0 -> infinity (exclusive) Node 0, Node 1, Node 2
     * Finally the stable layout is verified as well as the state transfer is verified by asserting
     * all 3 nodes' data.
     */
    @Test
    public void verifyStateTransferAndMergeInHeal() throws Exception {
        // Add three servers
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        addServerRule(SERVERS.PORT_2, new TestRule().matches(
                msg -> !msg.getMsgType().equals(CorfuMsgType.LAYOUT_BOOTSTRAP)
                        && !msg.getMsgType().equals(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST))
                .drop());

        final long writtenAddressesBatch1 = 3L;
        final long writtenAddressesBatch2 = 6L;
        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(writtenAddressesBatch2)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch2)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_2)
                .build();
        bootstrapAllServers(l1);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        setAggressiveTimeouts(l1, rt,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());
        setAggressiveDetectorTimeouts(SERVERS.PORT_0);
        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));

        // Write to address spaces 0 to 2 (inclusive) to SERVER 0 only.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Write to address spaces 3 to 5 (inclusive) to SERVER 0 and SERVER 1.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Write to address spaces 6 to 9 (inclusive) to SERVER 0 and SERVER 1.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Allow node 2 to be healed.
        clearServerRules(SERVERS.PORT_2);

        rt.invalidateLayout();

        // Wait until a stable and merged layout is observed
        waitForLayoutChange(layout -> layout.getUnresponsiveServers().isEmpty() &&
                        layout.segments.size() == 1,
                rt);

        final String[] expectedNodes = new String[]{SERVERS.ENDPOINT_0,
                SERVERS.ENDPOINT_1,
                SERVERS.ENDPOINT_2};
        final Layout actualLayout = rt.getLayoutView().getLayout();

        // Verify Node 1, Node 2, and Node 3 are active without considering order
        assertThat(actualLayout.getAllActiveServers()).containsExactlyInAnyOrder(expectedNodes);

        // Verify segments are merged into one segment
        assertThat(actualLayout.getSegments().size()).isEqualTo(1);

        // Verify start and end of the segment
        assertThat(actualLayout.getSegments().get(0).start).isEqualTo(0);
        assertThat(actualLayout.getSegments().get(0).end).isEqualTo(-1);

        // Verify no unresponsive server are in the layout
        assertThat(actualLayout.getUnresponsiveServers()).isEmpty();

        // Verify layout servers in the layout and their order
        assertThat(actualLayout.getLayoutServers()).containsExactly(expectedNodes);

        // Verify sequencers in the layout and their order
        assertThat(actualLayout.getSequencers()).containsExactly(expectedNodes);

        final long lastAddress = rt.getSequencerView().query(CorfuRuntime.getStreamID("test"));

        // Verify Nodes' data
        Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);
        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
    }

    /**
     * This test verifies that if the adjacent segments have same number of servers,
     * state transfer should not happen, while merge segments can succeed.
     * <p>
     * The test first creates a layout with 3 segments.
     * <p>
     * Segment 1: 0 -> 5 (exclusive) Node 0, Node 1
     * Segment 2: 5 -> 10 (exclusive) Node 0, Node 1
     * Segment 3: 10 -> infinity (exclusive) Node 0, Node 1
     * <p>
     * Now drop all the read response from Node 1, so that we make sure state transfer
     * will fail if it happens.
     * <p>
     * Finally verify merge segments succeed with only one segment in the new layout.
     */
    @Test
    public void verifyStateTransferNotHappenButMergeSucceeds() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        final long segmentSize = 5L;
        final int numSegments = 3;

        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(segmentSize)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(segmentSize)
                .setEnd(segmentSize * 2)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(segmentSize * 2)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();

        // Drop read responses to make sure state transfer will fail if it happens
        addServerRule(SERVERS.PORT_1, new TestRule().matches(m ->
                m.getMsgType().equals(CorfuMsgType.READ_RESPONSE)).drop());

        bootstrapAllServers(layout);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        for (int i = 0; i < segmentSize * numSegments; i++) {
            testStream.append("testPayload".getBytes());
        }

        // Verify that segments merged without state transfer
        waitForLayoutChange(l -> l.getSegments().size() == 1, rt);
    }

    /**
     * This test verifies that partial state transfers when retried only transfer the delta
     * of the address space.
     * Setup: Layout - Write 100 entries to SERVER 0 and 5 entries to SERVERS 0 & 1.
     * We then trigger the state transfer and force the transfer to fail half-way.
     * Another transfer is then triggered and verified that only the remaining data is transferred.
     */
    @Test
    public void verifyPartialStateTransferCompletionOnRetry() throws Exception {

        ServerContext sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                .setPort(SERVERS.PORT_0).build();
        addServer(SERVERS.PORT_0, sc1);

        ServerContext sc2 = new ServerContextBuilder()
                .setMemory(false)
                .setSingle(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1).build();
        addServer(SERVERS.PORT_1, sc2);

        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();

        final long writtenAddressesBatch1 = 100;
        final long writtenAddressesBatch2 = 5;

        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(-1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        setAggressiveTimeouts(layout, rt);

        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));

        for (int i = 0; i < (writtenAddressesBatch1 + writtenAddressesBatch2); i++) {
            testStream.append("testPayload".getBytes());
        }

        final long totalWritten = 50L;

        final long rangeWriteAllowedCount = (totalWritten / rt.getParameters().getBulkReadSize());

        AtomicLong allowedWrites = new AtomicLong(rangeWriteAllowedCount);

        // Allow only addresses 0 - 49 to be written.
        addClientRule(rt, SERVERS.ENDPOINT_1, new TestRule().matches(
                msg -> msg.getMsgType().equals(CorfuMsgType.RANGE_WRITE) &&
                        allowedWrites.decrementAndGet() < 0).drop());

        final RestoreRedundancyMergeSegments action1 = RestoreRedundancyMergeSegments
                .builder()
                .currentNode(SERVERS.ENDPOINT_1)
                .redundancyCalculator(new RedundancyCalculator(SERVERS.ENDPOINT_1))
                .build();

        // Assert that the TimeOutException is thrown
        assertThatThrownBy(() -> action1.impl(rt))
                .isInstanceOf(RetryExhaustedException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);

        // Known addresses should contain only [0:49] and the addresses in the open segment.
        ArrayList<Long> knownAddresses = new ArrayList<>(rt.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(SERVERS.ENDPOINT_1)
                .requestKnownAddresses(0L, writtenAddressesBatch1 + writtenAddressesBatch2)
                .get()
                .getKnownAddresses());

        List<Long> transferredRange = LongStream.range(0L, totalWritten).boxed().collect(Collectors.toList());
        final long start = 100L;
        final long end = 105L;

        List<Long> openSegmentData = LongStream.range(start, end).boxed().collect(Collectors.toList());

        transferredRange.addAll(openSegmentData);
        assertThat(knownAddresses).isEqualTo(transferredRange);
        // Reset the spy rules
        clearClientRules(rt);


        AtomicLong deltaBatches = new AtomicLong(0L);

        // Now count how many batches are transferred.
        addClientRule(rt, SERVERS.ENDPOINT_1, new TestRule().matches(
                msg -> msg.getMsgType().equals(CorfuMsgType.RANGE_WRITE) &&
                        deltaBatches.incrementAndGet() > 0));

        action1.impl(rt);

        knownAddresses = new ArrayList<>(rt.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(SERVERS.ENDPOINT_1)
                .requestKnownAddresses(0L, writtenAddressesBatch1 + writtenAddressesBatch2)
                .get()
                .getKnownAddresses());

        // Now all the addresses are known.
        assertThat(knownAddresses).isEqualTo(LongStream
                .range(0L, writtenAddressesBatch1 + writtenAddressesBatch2)
                .boxed().collect(Collectors.toList()));

        // Only the delta is transferred
        assertThat(deltaBatches.get() * rt.getParameters().getBulkReadSize())
                .isEqualTo(totalWritten);

    }

    private ServerContext configureContext(int port) {
        return new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(port))
                .setPort(port).build();
    }

    private RestoreRedundancyMergeSegments getRestoreAction(String endpoint) {
        return RestoreRedundancyMergeSegments
                .builder()
                .currentNode(endpoint)
                .redundancyCalculator(new RedundancyCalculator(endpoint))
                .build();
    }

    private CorfuRuntime getTestRuntime(int port, UUID nodeId) {
        return getNewRuntime(NodeLocator.builder()
                .host("test")
                .port(port)
                .nodeId(nodeId)
                .build()).connect();
    }

    private CompletableFuture<Void> runRestore(RestoreRedundancyMergeSegments restore, CorfuRuntime rt) {
        return CompletableFuture.runAsync(() -> {
            try {
                restore.impl(rt);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * This tests verifies that multiple running state transfers run to completion.
     */
    @Test
    public void verifyConcurrentCompletion() throws Exception {

        int server1 = SERVERS.PORT_0;
        int server2 = SERVERS.PORT_1;
        int server3 = SERVERS.PORT_2;

        ServerContext sc1 = configureContext(server1);

        addServer(server1, sc1);

        ServerContext sc2 = configureContext(server2);

        addServer(server2, sc2);

        ServerContext sc3 = configureContext(server3);

        addServer(server3, sc3);

        final long writtenAddressesBatch1 = 100;

        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(server1)
                .addLayoutServer(server2)
                .addLayoutServer(server3)
                .addSequencer(server1)
                .addSequencer(server2)
                .addSequencer(server3)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(server1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(-1)
                .buildStripe()
                .addLogUnit(server1)
                .addLogUnit(server2)
                .addLogUnit(server3)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(layout);


        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));

        for (int i = 0; i < (writtenAddressesBatch1); i++) {
            testStream.append("testPayload".getBytes());
        }

        final RestoreRedundancyMergeSegments action1 = getRestoreAction(SERVERS.ENDPOINT_1);
        final RestoreRedundancyMergeSegments action2 = getRestoreAction(SERVERS.ENDPOINT_2);


        List<CompletableFuture<Void>> futures = new ArrayList();

        CorfuRuntime rt1 = getTestRuntime(server2, sc1.getNodeId());
        CorfuRuntime rt2 = getTestRuntime(server3, sc2.getNodeId());

        futures.add(runRestore(action1, rt1));
        futures.add(runRestore(action2, rt2));

        waitForLayoutChange(l -> l.getSegments().size() == 1, rt);

        CFUtils.sequence(futures);

        rt.invalidateLayout();
        assertThat(rt.getLayoutView().getLayout().getSegments().size())
                .isEqualTo(1);
    }


    /**
     * Here we test the capability of state transfer mechanism to preserve codec info, regardless of the configuration
     * that state transfer runtime has.
     * <p>
     * This test first writes data with none default codecs (default being ZSTD), in the following way:
     * 0-2 [inclusive] write data to Node0 with codec LZ4
     * 3-5 [inclusive] write data to Node 0, Node1 with NO codec (NONE)
     * <p>
     * We then add a third node, and let state transfer complete. Finally, we read all the data to confirm
     * we are able to decompress all the data after state transfer.
     **/
    @Test
    public void testStateTransferWithNoneDefaultCodec() {

        final byte[] DEFAULT_PAYLOAD = "testPayload".getBytes();

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        final long writtenAddressesBatch1 = 3L;
        final long writtenAddressesBatch2 = 6L;

        // First Layout, with node0 and node1
        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l1);

        // Instantiate 2 different codecs, one for each none default codec type (state transfer will run with the
        // default runtime)
        corfuRuntime = getNewRuntime(getDefaultNode()).connect();
        corfuRuntime.getParameters().setCodecType(Codec.Type.LZ4);

        CorfuRuntime rtNoCodec = getNewRuntime(getDefaultNode()).connect();
        rtNoCodec.getParameters().setCodecType(Codec.Type.NONE);

        // Confirm that both codecs are different (we want to test all combinations) to the default codec (ZSTD)
        assertThat(corfuRuntime.getParameters().getCodecType())
                .isNotEqualTo(rtNoCodec.getParameters().getCodecType())
                .isNotEqualTo(CorfuRuntime.CorfuRuntimeParameters.builder().build().getCodecType());

        // Generate stream views for each codec type
        IStreamView testStream = corfuRuntime.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        IStreamView testStreamNoCodec = rtNoCodec.getStreamsView().get(CorfuRuntime.getStreamID("test"));

        // Write to address spaces 0 to 2 (inclusive) to SERVER 0 only with codec LZ4
        for (int i = 0; i < writtenAddressesBatch1; i++) {
            testStream.append(DEFAULT_PAYLOAD);
        }

        // Write to address spaces 3 to 5 (inclusive) to SERVER 0 and SERVER 1 with no Codec (None)
        for (int i = (int) writtenAddressesBatch1; i < writtenAddressesBatch2; i++) {
            testStreamNoCodec.append(DEFAULT_PAYLOAD);
        }

        // Add new Server
        addServer(SERVERS.PORT_2);
        final int addNodeRetries = 3;
        corfuRuntime.getManagementView()
                .addNode(SERVERS.ENDPOINT_2, addNodeRetries, Duration.ofMinutes(1L), Duration.ofSeconds(1));
        corfuRuntime.invalidateLayout();
        final long epochAfterAdd = 4L;
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(epochAfterAdd)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        // Perform State Transfer
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() >= epochAfterAdd) {
                break;
            }
            corfuRuntime.invalidateLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
        }

        assertThat(corfuRuntime.getLayoutView().getLayout()).isEqualTo(expectedLayout);

        // Read Runtime (fresh runtime)
        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        // First Invalidate Server & Client Caches
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();

        // Read data to validate we are able to decompress correctly
        for (int i = 0; i < writtenAddressesBatch2; i++) {
            assertThat(rt.getAddressSpaceView().read(i).getPayload()).isEqualTo(DEFAULT_PAYLOAD);
        }

    }
}
