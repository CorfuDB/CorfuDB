package org.corfudb.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.ThreadSafeStreamView;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.StreamOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.UUID;

import static org.corfudb.infrastructure.ServerContext.PLUGIN_CONFIG_FILE_PATH;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

public class CorfuLogReplicationRuntimeTest {

    private final String sampleCluster = "CLUSTER";

    private CorfuLogReplicationRuntime corfuLogReplicationRuntime;

    private volatile Runnable systemDownHandler = () -> {
    };

    @Before
    public void setup() {
        int maxWriteSize = 25 << 20;
        long tail = 0L;
        final String clusterId = "0-0-0-0-0";
        final String keyStore = "--keystore";
        final String ksPasswordFile = "--ks_password";
        final String localCorfuEndPoint = "localhost:9000";
        final String lrTransactionStream = "LR_Transaction_Stream";
        final String stream = "stream";
        final String trustStore = "--truststore";
        final String trustStorePassword = "--truststore_password";
        UUID streamId = UUID.nameUUIDFromBytes(lrTransactionStream.getBytes());

        ClusterDescriptor clusterDescriptor = mock(ClusterDescriptor.class);
        LogReplicationRuntimeParameters lrRuntimeParameters = mock(LogReplicationRuntimeParameters.class);
        doReturn(clusterId).when(clusterDescriptor).getClusterId();
        doReturn(clusterDescriptor).when(lrRuntimeParameters).getRemoteClusterDescriptor();
        doReturn(trustStore).when(lrRuntimeParameters).getTrustStore();
        doReturn(trustStorePassword).when(lrRuntimeParameters).getTsPasswordFile();
        doReturn(keyStore).when(lrRuntimeParameters).getKeyStore();
        doReturn(ksPasswordFile).when(lrRuntimeParameters).getKsPasswordFile();
        doReturn(localCorfuEndPoint).when(lrRuntimeParameters).getLocalCorfuEndpoint();
        doReturn(maxWriteSize).when(lrRuntimeParameters).getMaxWriteSize();
        doReturn(true).when(lrRuntimeParameters).isTlsEnabled();

        doReturn(PLUGIN_CONFIG_FILE_PATH).when(lrRuntimeParameters).getPluginFilePath();
        doReturn(sampleCluster).when(lrRuntimeParameters).getLocalClusterId();

        LogReplicationConfig config = mock(LogReplicationConfig.class);
        Set<String> streamsToReplicate = new HashSet<>();
        streamsToReplicate.add(stream);

        doReturn(streamsToReplicate).when(config).getStreamsToReplicate();

        doReturn(config).when(lrRuntimeParameters).getReplicationConfig();

        LogReplicationMetadataManager metadataManager = mock(LogReplicationMetadataManager.class);
        LogReplicationConfigManager replicationConfigManager = mock(LogReplicationConfigManager.class);
        CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);

        List<String> layoutServers = new ArrayList<>();
        layoutServers.add(localCorfuEndPoint);

        doReturn(layoutServers).when(corfuRuntime).getLayoutServers();

        try (MockedStatic mockCorfuRuntime = mockStatic(CorfuRuntime.class)) {
            mockCorfuRuntime.when(() -> CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                    .trustStore(trustStore)
                    .tsPasswordFile(trustStorePassword)
                    .keyStore(trustStorePassword)
                    .ksPasswordFile(ksPasswordFile)
                    .systemDownHandler(systemDownHandler)
                    .tlsEnabled(true)
                    .cacheDisabled(true)
                    .maxWriteSize(maxWriteSize)
                    .build())).thenReturn(corfuRuntime);

            doReturn(corfuRuntime).when(corfuRuntime).parseConfigurationString(localCorfuEndPoint);
            doReturn(corfuRuntime).when(corfuRuntime).connect();

            mockCorfuRuntime.when(() -> CorfuRuntime.getStreamID(lrTransactionStream)).thenReturn(streamId);

            StreamsView mockStreamView = mock(StreamsView.class);
            doReturn(mockStreamView).when(corfuRuntime).getStreamsView();
            IStreamView mockIStreamView = mock(IStreamView.class);
            doReturn(mockIStreamView).when(mockStreamView).get(UUID.nameUUIDFromBytes(lrTransactionStream.getBytes()));
            Stream mockStream = mock(Stream.class);
            doReturn(mockStream).when(mockIStreamView).streamUpTo(tail);

            LayoutView mockLayoutView = mock(LayoutView.class);
            doReturn(mockLayoutView).when(corfuRuntime).getLayoutView();
            Layout mockLayout = mock(Layout.class);
            doReturn(mockLayout).when(mockLayoutView).getLayout();
            Layout.LayoutSegment mockLayoutSegment = mock(Layout.LayoutSegment.class);
            doReturn(mockLayoutSegment).when(mockLayout).getLastSegment();
            Layout.ReplicationMode mockReplicationMode = mock(Layout.ReplicationMode.class);
            doReturn(mockReplicationMode).when(mockLayoutSegment).getReplicationMode();

            AddressSpaceView mockAddressSpaceView = mock(AddressSpaceView.class);
            doReturn(mockAddressSpaceView).when(corfuRuntime).getAddressSpaceView();
            doReturn(tail).when(mockAddressSpaceView).getLogTail();

            StreamOptions option = StreamOptions.builder()
                    .ignoreTrimmed(false)
                    .cacheEntries(true)
                    .isCheckpointCapable(true)
                    .build();

            doReturn(new ThreadSafeStreamView(corfuRuntime, streamId, option)).
                    when(mockReplicationMode)
                    .getStreamView(corfuRuntime, streamId, option);

            corfuLogReplicationRuntime = spy(new CorfuLogReplicationRuntime(lrRuntimeParameters,
                    metadataManager, replicationConfigManager));
        }
    }

    /**
     * Test resetRemoteLeaderNodeId()
     */
    @Test
    public void testResetRemoteLeaderNodeId() {
        corfuLogReplicationRuntime.resetRemoteLeaderNodeId();
        Assert.assertEquals(Optional.empty(), corfuLogReplicationRuntime.getRemoteLeaderNodeId());
    }

    /**
     * Test setRemoteLeaderNodeId()
     */
    @Test
    public void testSetRemoteLeaderNodeId() {
        String nodeId = "id";
        corfuLogReplicationRuntime.setRemoteLeaderNodeId(nodeId);
        Assert.assertEquals(nodeId,corfuLogReplicationRuntime.getRemoteLeaderNodeId().get());
    }

    /**
     * Test updateConnectedNodes() and updateDisconnectedNodes()
     */
    @Test
    public void testConnectedNodes() {
        String nodeId = "nodeId";
        Set<String> nodes = new HashSet<String>();
        nodes.add(nodeId);
        corfuLogReplicationRuntime.updateConnectedNodes(nodeId);
        Assert.assertEquals(corfuLogReplicationRuntime.getConnectedNodes(), nodes);
        corfuLogReplicationRuntime.updateDisconnectedNodes(nodeId);
        Assert.assertEquals(corfuLogReplicationRuntime.getConnectedNodes(), new HashSet<>());
    }

    /**
     * Test updateRouterClusterDescriptor
     */
    @Test
    public void testUpdateRouterClusterDescriptor() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(CorfuLogReplicationRuntime.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        corfuLogReplicationRuntime.start();
        ClusterDescriptor newClusterDescriptor = new ClusterDescriptor(sampleCluster, LogReplicationClusterInfo.ClusterRole.ACTIVE, 9000);
        corfuLogReplicationRuntime.updateRouterClusterDescriptor(newClusterDescriptor);
        corfuLogReplicationRuntime.stop();
        List<ILoggingEvent> logsList = listAppender.list;
        Assert.assertEquals("update router's cluster descriptor Cluster[CLUSTER]:: role=ACTIVE, CorfuPort=9000, Nodes[0]:  ", logsList.get(1)
                .getFormattedMessage());
    }
}
