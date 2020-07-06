package org.corfudb.integration;

import lombok.val;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.util.Sleep;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

public class CorfuReplicationClusterRoleChangeIT extends CorfuLogReplicationAbstractIT {

    private final static int WAIT_FACTOR = 5;

    private final static String nettyFileMonitorConfig = "src/test/resources/transport/fileMonitorNettyConfig.properties";

    /**
     * Test stable cluster role change, i.e., allow snapshot and log entry sync to complete
     * from initial active cluster. Once sync is complete, enforce cluster role change, i.e.,
     * current active will become standby and current standby will become the new active.
     * Write new data to the cluster and verify old and new data is present in the new standby.
     *
     * @throws Exception
     */
    @Test
    public void testLogReplicationClusterRoleChange() throws Exception {

        this.pluginConfigFilePath = getPluginConfig(nettyFileMonitorConfig);

        // Use FileMonitorClusterManagerAdapter for testing
        // This testing adapter will enforce a cluster role switch by monitoring the topology file until a change occurs
        testEndToEndSnapshotAndLogEntrySync(true);

        System.out.println("\nEnforce cluster role change... increase topology config id...");

        TopologyDescriptor initTopology = FileMonitorClusterManagerAdapter.readTopologyFromJSONFile(TOPOLOGY_TEST_FILE);

        // Modify .json to enforce cluster role switch
        // When FileMonitorClusterManagerAdapter perceives the topologyConfigId change
        // it will call on active to become standby, once the cluster status shows 100%
        // the topology will be changed and roles will switch.
        increaseTopologyConfigId(TOPOLOGY_TEST_FILE);

        System.out.println("Wait for cluster role change to happen...");
        // Wait for n intervals of the monitor, to be sure the change happened
        Sleep.sleepUninterruptibly(Duration.ofMillis(FileMonitorClusterManagerAdapter.MONITOR_INTERVAL*WAIT_FACTOR));

        // Verify Topology Roles Changed
        System.out.println("**** Read new topology");
        TopologyDescriptor newTopology = FileMonitorClusterManagerAdapter.readTopologyFromJSONFile(TOPOLOGY_TEST_FILE);

        System.out.println("**** Verify new topology");
        // Confirm topologyConfigId changed for the new topology
        assertThat(newTopology.getTopologyConfigId()).isGreaterThan(initTopology.getTopologyConfigId());
        // Confirm new active is former standby
        assertThat(newTopology.getActiveClusters().values().iterator().next().getClusterId())
                .isEqualTo(initTopology.getStandbyClusters().values().iterator().next().getClusterId());
        // Confirm new standby is former active
        assertThat(newTopology.getStandbyClusters().values().iterator().next().getClusterId())
                .isEqualTo(initTopology.getActiveClusters().values().iterator().next().getClusterId());

        System.out.println("**** Verify data on new active cluster, before writing more data");
        verifyDataOnStandby(numWrites + (numWrites/2));

        // Write to former standby which is the current active
        System.out.println("**** Write to new active cluster");
        writeToStandby((numWrites + (numWrites / 2)), numWrites);

        // Verify Data Present on former standby (current active) to which data was written
        System.out.println("**** Verify data on new active cluster");
        verifyDataOnStandby(numWrites*2 + (numWrites/2));

        // Verify Data Present on former standby (current active)
        System.out.println("**** Verify data on new standby");
        verifyDataOnActive(numWrites*2 + (numWrites/2));

        System.out.println("Test succeeds");
    }

    /**
     * Test Log Replication across clusters, when the configuration
     * provided by the cluster manager is not a clean start, i.e.,
     * topology config id != 0
     */
    @Test
    public void testLogReplicationForOngoingTopologyConfigId() throws Exception {

        this.pluginConfigFilePath = getPluginConfig(nettyFileMonitorConfig);

        final int newTopologyConfigId = 5;

        // Modify Base Topology to start with topologyConfigId = 20;
        TopologyDescriptor baseTopology = FileMonitorClusterManagerAdapter.readTopologyFromJSONFile(TOPOLOGY_BASE_FILE);
        baseTopology.setTopologyConfigId(newTopologyConfigId);
        writeTopologyToJSONFile(baseTopology, TOPOLOGY_TEST_FILE);

        // Run End-to-End Log Replication Test
        testEndToEndSnapshotAndLogEntrySync(false);
    }
}
