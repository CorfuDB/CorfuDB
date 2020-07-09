package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationTopologyInfoStore;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationID;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyTimestamp;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ReplicationTopologyInfoStoreTest extends AbstractViewTest {

    @Test
    public void testAppendAndQuery() {
        CorfuRuntime runtime = getDefaultRuntime().connect();
        DefaultClusterManager defaultClusterManager = new DefaultClusterManager();
        ReplicationTopologyInfoStore topologyInfoStore = new ReplicationTopologyInfoStore(runtime, UUID.randomUUID().toString());

        // Get some configMsg and write the table
        TopologyConfigurationMsg topologyConfigurationMsg = defaultClusterManager.queryTopologyConfig();

        // Not data in the table
        Collection<CorfuStoreEntry<TopologyTimestamp, TopologyConfigurationMsg, TopologyConfigurationID>> queryResult =
                topologyInfoStore.query(topologyConfigurationMsg.getTopologyConfigID());
        System.out.print("\nnumber of entries " + queryResult.size());
        assertThat(queryResult.size()).isEqualTo(0);


        // Append one topologyConfig
        topologyInfoStore.append(topologyConfigurationMsg);
        queryResult =
                topologyInfoStore.query(topologyConfigurationMsg.getTopologyConfigID());
        System.out.print("\nnumber of entries " + queryResult.size());
        assertThat(queryResult.size()).isEqualTo(1);

        // Append another topologyConfig with the same configID
        topologyInfoStore.append(topologyConfigurationMsg);
        queryResult =
                topologyInfoStore.query(topologyConfigurationMsg.getTopologyConfigID());
        System.out.print("\nnumber of entries " + queryResult.size());
        assertThat(queryResult.size()).isEqualTo(2);

        TopologyConfigurationMsg newTopologyConfigurationMsg = TopologyConfigurationMsg.newBuilder()
                .setTopologyConfigID(topologyConfigurationMsg.getTopologyConfigID() + 1)
                .build();
        topologyInfoStore.append(newTopologyConfigurationMsg);
        queryResult =
                topologyInfoStore.query(topologyConfigurationMsg.getTopologyConfigID());
        assertThat(queryResult.size()).isEqualTo(2);

        queryResult =
                topologyInfoStore.query(topologyConfigurationMsg.getTopologyConfigID() + 1);
        assertThat(queryResult.size()).isEqualTo(1);
    }
}
