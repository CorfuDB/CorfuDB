package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class LogReplicationAbstractIT extends AbstractIT {

    public final static String nettyConfig = "src/test/resources/transport/nettyConfig.properties";

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

    public void testEndToEndSnapshotAndLogEntrySync() throws Exception {
        try {
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

    public void writeToActiveNonTx(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            mapA.put(String.valueOf(i), i);
        }
    }

    public void startLogReplicatorServers() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, pluginConfigFilePath);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, pluginConfigFilePath);
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

    public void stopActiveLogReplicator() {
        List<String> paramsPs = Arrays.asList("/bin/sh", "-c", "ps aux | grep CorfuInterClusterReplicationServer | grep 9010");
        String result = runCommandForOutput(paramsPs);

        // Get PID
        String[] output = result.split(" ");
        int i = 0;
        String pid = "";
        for (String st : output) {
            if (!st.equals("")) {
                i++;
                if (i == 2) {
                    pid = st;
                    break;
                }
            }
        }

        System.out.println("*** Active PID :: " + pid);

        List<String> paramsKill = Arrays.asList("/bin/sh", "-c", "kill -9 " + pid);
        runCommandForOutput(paramsKill);

        if (activeReplicationServer != null) {
            activeReplicationServer.destroyForcibly();
            activeReplicationServer = null;
        }
    }

    public static String runCommandForOutput(List<String> params) {
        ProcessBuilder pb = new ProcessBuilder(params);
        Process p;
        String result = "";
        try {
            p = pb.start();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
            reader.lines().iterator().forEachRemaining(sj::add);
            result = sj.toString();

            p.waitFor();
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void startActiveLogReplicator() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, pluginConfigFilePath);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=localhost", String.valueOf(activeReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            System.out.println("Error caught while running Log Replication Server");
        }
    }

    public void verifyDataOnStandby(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapAStandby.size() != expectedConsecutiveWrites) {
            // Block until expected number of entries is reached
        }

        // Verify data is present in Standby Site
        assertThat(mapAStandby.size()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            assertThat(mapAStandby.containsKey(String.valueOf(i)));
        }
    }

    public void checkpoint(CorfuRuntime rt, List<CorfuTable> toCheckpoint) {
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addAllMaps(toCheckpoint);
        Token trimMark = mcw.appendCheckpoints(rt, "author");
        rt.getAddressSpaceView().prefixTrim(trimMark);
        rt.getAddressSpaceView().invalidateClientCache();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().gc();
    }

    public Token checkpointAndTrimCorfuStore() {

        CorfuRuntime runtimeC = new CorfuRuntime(activeEndpoint).connect();

        TableRegistry tableRegistry = runtimeC.getTableRegistry();
        CorfuTable<CorfuStoreMetadata.TableName,
                CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                                        CorfuStoreMetadata.TableMetadata>>
                tableRegistryCT = tableRegistry.getRegistryTable();

        // Save the regular serializer first..
        ISerializer protobufSerializer = Serializers.getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtobufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtobufSerializer
        ISerializer dynamicProtobufSerializer = new DynamicProtobufSerializer(runtimeC);
        Serializers.registerSerializer(dynamicProtobufSerializer);

        // First checkpoint the TableRegistry system table
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();

        Token trimToken = new Token(Token.UNINITIALIZED.getEpoch(), Token.UNINITIALIZED.getSequence());
        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            System.out.println("**** Checkpoint :: " + tableName.getTableName());
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                    tableName.getNamespace(), tableName.getTableName()
            );
            SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder = runtimeC.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                    .setStreamName(fullTableName)
                    .setSerializer(dynamicProtobufSerializer);

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());
            Token token = mcw.appendCheckpoints(runtimeC, "checkpointer");
            trimToken = Token.min(trimToken, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(runtimeC, "checkpointer");
        trimToken = Token.min(trimToken, token);

        runtimeC.getAddressSpaceView().prefixTrim(trimToken);
        runtimeC.getAddressSpaceView().gc();

        // Lastly restore the regular protobuf serializer and undo the dynamic protobuf serializer
        // otherwise the test cannot continue beyond this point.
        Serializers.registerSerializer(protobufSerializer);
        return trimToken;
    }

    public void checkpointAndTrimStandby() {
        // Standby MetadataTable to be checkpointed
        String metadataTableName = LogReplicationMetadataManager.TABLE_PREFIX_NAME + DefaultClusterManager.DEFAULT_STANDBY_CLUSTER_NAME;
        CorfuTable<LogReplicationMetadata.LogReplicationMetadataKey, LogReplicationMetadata.LogReplicationMetadataVal> metadataTableStandby =
                standbyRuntime.getObjectsView().build()
                        .setTypeToken(new TypeToken<CorfuTable<LogReplicationMetadata.LogReplicationMetadataKey,
                                LogReplicationMetadata.LogReplicationMetadataVal>>() {
                        })
                        .setStreamName(TableRegistry.getFullyQualifiedTableName(LogReplicationMetadataManager.NAMESPACE, metadataTableName))
                        .open();

        List<CorfuTable> toCheckpoint = new ArrayList<>();
        toCheckpoint.add(mapAStandby);
        toCheckpoint.add(metadataTableStandby);
        System.out.println("**** Checkpoint and trim ... on standby");
        CorfuRuntime standbyCPRuntime = new CorfuRuntime(standbyEndpoint).connect();
        checkpoint(standbyCPRuntime, toCheckpoint);
    }
}