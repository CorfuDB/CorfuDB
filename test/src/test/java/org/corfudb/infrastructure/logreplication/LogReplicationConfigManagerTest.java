package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LogReplicationConfigManagerTest extends AbstractViewTest {

    private CorfuRuntime runtime;
    private final String nettyPluginPath = "./test/src/test/resources/transport/nettyConfig.properties";

    @Before
    public void setUp() {
        runtime = getDefaultRuntime();
    }

    @Test
    public void testGetSubscriberToStreamsMap() {
        LogReplicationConfigManager replicationConfigManager = new LogReplicationConfigManager(runtime,
            nettyPluginPath);

        Map<ReplicationSubscriber, Set<String>> subscriberToStreamsMap =
            replicationConfigManager.getSubscriberToStreamsMap();

        // Verify that an entry exists for each supported replication model
        Set<LogReplicationMetadata.ReplicationModels> modelsInConfig =
            subscriberToStreamsMap.keySet().stream().map(key -> key.getReplicationModel()).collect(Collectors.toSet());
        // TODO: this will be un-commented as we use the replication Models. Currently, we use only 1 out of the four replication models.
        Assert.assertTrue(modelsInConfig.size() == 1 && modelsInConfig.contains(LogReplicationMetadata.ReplicationModels.REPLICATE_FULL_TABLES));

        // Verify that all streams returned by the default config adapter are present in the constructed config
        // TODO pankti: After the change to read options from the registry table is available, verification must be
        //  done against the opened tables and not the default config returned by the adapter.
        Map<ReplicationSubscriber, Set<String>> expectedMap = new DefaultLogReplicationConfigAdapter()
            .getSubscriberToStreamsMap();

        expectedMap.forEach((subscriber, streams) -> {
            Assert.assertTrue(subscriberToStreamsMap.containsKey(subscriber));
            Assert.assertTrue(Objects.equals(subscriberToStreamsMap.get(subscriber), streams));
        });
    }

    @After
    public void cleanUp() {
        runtime.shutdown();
    }
}
