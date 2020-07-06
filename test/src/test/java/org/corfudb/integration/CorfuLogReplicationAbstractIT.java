package org.corfudb.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuLogReplicationAbstractIT extends AbstractIT {

    private final static String nettyConfig = "src/test/resources/transport/nettyConfig.properties";

    public final static String TOPOLOGY_BASE_FILE = "src/test/resources/topology-sample-do-not-modify.json";
    public final static String TOPOLOGY_TEST_FILE = "src/test/resources/topology-test.json";

    public String pluginConfigFilePath;

    // Note: this flag is kept for debugging purposes only.
    // Log Replication Server should run as a process as the unexpected termination of it
    // (for instance the completion of a test) causes SYSTEM_EXIT(ERROR_CODE).
    // If flipped to debug (easily access logs within the IDE, flip back before pushing upstream).
    public static boolean runProcess = true;

    public ExecutorService executorService = Executors.newFixedThreadPool(2);
    public Process activeCorfu = null;
    public Process standbyCorfu = null;

    public Process activeReplicationServer = null;
    public Process standbyReplicationServer = null;

    public final String streamA = "Table001";

    public final int numWrites = 10;

    public final int activeSiteCorfuPort = 9000;
    public final int standbySiteCorfuPort = 9001;

    public final int activeReplicationServerPort = 9010;
    public final int standbyReplicationServerPort = 9020;

    public final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    public final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    public CorfuRuntime activeRuntime;
    public CorfuRuntime standbyRuntime;

    public CorfuTable<String, Integer> mapA;
    public CorfuTable<String, Integer> mapAStandby;

    public void testEndToEndSnapshotAndLogEntrySync(boolean defaultTopology) throws Exception {
        try {
            // Refresh topology file to default topology (in case it was modified by any other test)
            // Default Topology: topologyConfigId=0
            // Active Cluster:
            //      - LR :: localhost:9010
            //      - CorfuDB :: localhost:9000
            // Standby Cluster:
            //      - LR :: localhost:9020
            //      - CorfuDB :: localhost:9001
            if (defaultTopology) {
                resetDefaultTopology();
            }

            System.out.println("Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            System.out.println("Open map on active and standby");
            openMap();

            System.out.println("Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWrites);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.size()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Cluster
            assertThat(mapAStandby.size()).isEqualTo(0);

            startLogReplicatorServers();

            System.out.println("Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandby(numWrites);

            // Add Delta's for Log Entry Sync
            writeToActive(numWrites, numWrites/2);

            System.out.println("Wait ... Delta log replication in progress ...");
            verifyDataOnStandby((numWrites + (numWrites / 2)));
        } finally {

            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }

    }

    /**
     * Reset topology-test.json (file used to read topology in the case of ITs)
     * Copy from base file topology-sample-do-not-modify.json
     */
    private void resetDefaultTopology() throws IOException {
        File baseTopology = new File(TOPOLOGY_BASE_FILE);
        File testTopology = new File(TOPOLOGY_TEST_FILE);
        testTopology.delete();
        Files.copy(baseTopology.toPath(), testTopology.toPath());
    }

    public void setupActiveAndStandbyCorfu() throws Exception {
        try {
            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            // Setup runtime's to active and standby Corfu
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime.parseConfigurationString(activeEndpoint);
            activeRuntime.connect();

            standbyRuntime = new CorfuRuntime(standbyEndpoint).connect();
        } catch (Exception e) {
            System.out.println("Error while starting Corfu");
            throw  e;
        }
    }

    public void openMap() {
        // Write to StreamA on Active Site
        mapA = activeRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        mapAStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(mapA.size()).isEqualTo(0);
        assertThat(mapAStandby.size()).isEqualTo(0);
    }

    public void writeToActive(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapA.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
    }

    public void writeToStandby(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            standbyRuntime.getObjectsView().TXBegin();
            mapAStandby.put(String.valueOf(i), i);
            standbyRuntime.getObjectsView().TXEnd();
        }
    }

    public void startLogReplicatorServers() {
        try {
            if (runProcess) {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=localhost", String.valueOf(activeReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=localhost", String.valueOf(standbyReplicationServerPort)});
                });
//                // Start Log Replication Server on Active Site
//                activeReplicationServer = runReplicationServer(activeReplicationServerPort, pluginConfigFilePath);
//
//                // Start Log Replication Server on Standby Site
//                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, pluginConfigFilePath);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=localhost", String.valueOf(activeReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=localhost", String.valueOf(standbyReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            System.out.println("Error caught while running Log Replication Server");
        }
    }

    public void verifyDataOnStandby(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapAStandby.size() != expectedConsecutiveWrites) {
            //
            System.out.println(mapAStandby.size() + "::" + expectedConsecutiveWrites);
        }

        // Verify data is present in Standby Site
        assertThat(mapAStandby.size()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            assertThat(mapAStandby.containsKey(String.valueOf(i)));
        }
    }

    public void verifyDataOnActive(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapA.size() != expectedConsecutiveWrites) {
            //
        }

        // Verify data is present in Standby Site
        assertThat(mapA.size()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            assertThat(mapA.containsKey(String.valueOf(i)));
        }
    }

    public void increaseTopologyConfigId(String topologyFile) {
        // Read JSON File and increase the topologyConfigId
        JSONParser parser = new JSONParser();
        try {
            TopologyDescriptor topology = FileMonitorClusterManagerAdapter.readTopologyFromJSONFile(topologyFile);
            long newTopologyConfigId = topology.getTopologyConfigId() + 1;
            System.out.println("Former topologyConfigId=" + topology.getTopologyConfigId() + " new TopologyConfigId=" + newTopologyConfigId);
            TopologyDescriptor newTopology = new TopologyDescriptor(newTopologyConfigId,
                    new ArrayList<>(topology.getActiveClusters().values()),
                    new ArrayList<>(topology.getStandbyClusters().values()));
//
//            Object object = parser.parse(new FileReader(topologyFile));
//
//            // Topology as JSON
//            JSONObject topologyObject = (JSONObject) object;
//            long currentTopologyConfigId = (long) topologyObject.get("topologyConfigId");
//
//            // Update TopologyConfigId
//            currentTopologyConfigId++;
//            topologyObject.put("topologyConfigId", currentTopologyConfigId);


            ObjectMapper mapper = new ObjectMapper();

            File file = new File(topologyFile);
            try {
                // Serialize Java object info JSON file.
                mapper.writeValue(file, newTopology);
            } catch (IOException e) {
                e.printStackTrace();
            }

//            // Write to file
//            try (FileWriter file = new FileWriter(topologyFile)) {
//                file.write(topologyObject.toJSONString());
//                System.out.println("Increased topology config id to :: " + currentTopologyConfigId);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        } catch (Exception e) {
            //
        }
    }

    public void writeTopologyToJSONFile(TopologyDescriptor topology, String topologyFile) {
        ObjectMapper mapper = new ObjectMapper();

        File file = new File(topologyFile);
        try {
            // Serialize Java object info JSON file.
            mapper.writeValue(file, topology);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Return Default Plugin, using nettyConfig.properties in resource folder as default
     *
     * @return default plugin config file to be used for this test
     */
    public String getDefaultPluginConfig() {
        return getPluginConfig(nettyConfig);
    }

    /**
     * Return Default Plugin, using nettyConfig.properties in resource folder as default
     *
     * @return default plugin config file to be used for this test
     */
    public String getPluginConfig(String configFilePath) {
        File f = new File(configFilePath);
        return f.getAbsolutePath();
    }

    /**
     * Utility function to get a Topology as a JSON file
     *
     * @throws IOException
     */
//    public void topologyToJSON(String outputFile) throws IOException {
//        String activeClusterId = UUID.randomUUID().toString();
//        NodeDescriptor nodeActive = new NodeDescriptor("10.10.10.10", "9010", activeClusterId,
//                UUID.randomUUID());
//        ClusterDescriptor activeCluster = new ClusterDescriptor(activeClusterId, LogReplicationClusterInfo.ClusterRole.ACTIVE, 9000);
//        activeCluster.addNode(nodeActive);
//
//        String standbyClusterId = UUID.randomUUID().toString();
//        Map<String, ClusterDescriptor> standbyClusters = new HashMap<>();
//        NodeDescriptor standbyNode = new NodeDescriptor("20.20.20.20", "9020", standbyClusterId,
//                UUID.randomUUID());
//        ClusterDescriptor standbyCluster= new ClusterDescriptor(standbyClusterId, LogReplicationClusterInfo.ClusterRole.STANDBY, 9000);
//        standbyCluster.addNode(standbyNode);
//        standbyClusters.put(standbyClusterId, standbyCluster);
//
//        TopologyDescriptor topology = new TopologyDescriptor(0, activeCluster, standbyClusters);
//
//        ObjectMapper mapper = new ObjectMapper();
//
//        File file = new File(outputFile);
//        try {
//            // Serialize Java object info JSON file.
//            mapper.writeValue(file, topology);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

}
