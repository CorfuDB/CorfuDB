package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.utils.CommonTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED;


/**
 * This test suite exercises some topology config change scenarios.
 * Each test will start with two single node corfu servers, and two single node log replicators.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuReplicationClusterConfigIT extends AbstractIT {
    public final static String nettyPluginPath = "src/test/resources/transport/nettyConfig.properties";
    private final static String streamName = "Table001";

    private final static long shortInterval = 1L;
    private final static long mediumInterval = 10L;
    private final static int firstBatch = 10;
    private final static int secondBatch = 15;
    private final static int thirdBatch = 20;
    private final static int fourthBatch = 25;
    private final static int largeBatch = 50;

    private final static int activeClusterCorfuPort = 9000;
    private final static int standbyClusterCorfuPort = 9001;
    private final static int activeReplicationServerPort = 9010;
    private final static int standbyReplicationServerPort = 9020;
    private final static String activeCorfuEndpoint = DEFAULT_HOST + ":" + activeClusterCorfuPort;
    private final static String standbyCorfuEndpoint = DEFAULT_HOST + ":" + standbyClusterCorfuPort;

    private Process activeCorfuServer = null;
    private Process standbyCorfuServer = null;
    private Process activeReplicationServer = null;
    private Process standbyReplicationServer = null;

    private CorfuRuntime activeRuntime;
    private CorfuRuntime standbyRuntime;
    private CorfuTable<String, Integer> mapActive;
    private CorfuTable<String, Integer> mapStandby;

    private CorfuStore corfuStore;
    private Table<CommonTypes.Uuid, CommonTypes.Uuid, CommonTypes.Uuid> configTable;

    @Before
    public void setUp() throws Exception {
        activeCorfuServer = runServer(activeClusterCorfuPort, true);
        standbyCorfuServer = runServer(standbyClusterCorfuPort, true);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(activeCorfuEndpoint).connect();

        standbyRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        standbyRuntime.parseConfigurationString(standbyCorfuEndpoint).connect();

        mapActive = activeRuntime.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        mapStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(mapActive.size()).isZero();
        assertThat(mapStandby.size()).isZero();

        corfuStore = new CorfuStore(activeRuntime);

        configTable = corfuStore.openTable(
                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                CommonTypes.Uuid.class, CommonTypes.Uuid.class, CommonTypes.Uuid.class,
                TableOptions.builder().build()
        );
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        if (activeRuntime != null) {
            activeRuntime.shutdown();
        }

        if (standbyRuntime != null) {
            standbyRuntime.shutdown();
        }

        shutdownCorfuServer(activeCorfuServer);
        shutdownCorfuServer(standbyCorfuServer);
        shutdownCorfuServer(activeReplicationServer);
        shutdownCorfuServer(standbyReplicationServer);
    }

    /**
     * This test verifies config change with a role switch.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a role switch with corfu store
     * 7. Write 5 more entries to standby map, which becomes source right now.
     * 8. Verify data will be replicated in reverse direction.
     */
    @Test
    public void testNewConfigWithSwitchRole() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }
        log.info("Log replication succeeds without config change!");

        // Perform a role switch
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_SWITCH,
                        DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH)
                .commit();
        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapStandby
        for (int i = secondBatch; i < thirdBatch; i++) {
            standbyRuntime.getObjectsView().TXBegin();
            mapStandby.put(String.valueOf(i), i);
            standbyRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapStandby.size()).isEqualTo(thirdBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == thirdBatch, mapActive, thirdBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                thirdBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(thirdBatch);

        // Second Role Switch
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_SWITCH,
                        DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH)
                .commit();
        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapStandby
        for (int i = thirdBatch; i < fourthBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(fourthBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == fourthBatch, mapStandby, fourthBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                fourthBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(fourthBatch);
        assertThat(mapStandby.size()).isEqualTo(fourthBatch);
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync transfer phase.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 50 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Perform a role switch with corfu store
     * 5. Standby will drop messages and keep size 0
     * 6. Verify active map becomes size 0, since source size is 0
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringTransferPhase() throws Exception {
        // Write 50 entry to active map
        for (int i = 0; i < largeBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(largeBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", largeBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");
        TimeUnit.SECONDS.sleep(shortInterval);

        // Perform a role switch during transfer
        assertThat(mapStandby.size()).isEqualTo(0);
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_SWITCH,
                        DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH)
                .commit();
        assertThat(configTable.count()).isOne();
        assertThat(mapStandby.size()).isEqualTo(0);

        // Wait until active map size becomes 0
        waitForReplication(size -> size == 0, mapActive, 0);
        log.info("After role switch during transfer phase, both maps have size {}. Current " +
                        "active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                mapActive.size(), activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isZero();
        assertThat(mapStandby.size()).isZero();
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync apply phase.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 50 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync goes to apply phase
     * 5. Perform a role switch with corfu store
     * 6. Standby will continue apply and have size 50
     * 7. Verify both maps have size 50
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringApplyPhase() throws Exception {
        // Write 50 entry to active map
        for (int i = 0; i < largeBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(largeBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", largeBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until apply phase
        UUID standbyStream = CorfuRuntime.getStreamID(streamName);
        while (!standbyRuntime.getAddressSpaceView().getAllTails().getStreamTails().containsKey(standbyStream)) {
            TimeUnit.MILLISECONDS.sleep(100L);
        }

        log.info("======standby tail is : " + standbyRuntime.getAddressSpaceView().getAllTails().getStreamTails().get(standbyStream));

        // Perform a role switch
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_SWITCH,
                        DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH)
                .commit();
        assertThat(configTable.count()).isOne();

        // Should finish apply
        waitForReplication(size -> size == largeBatch, mapStandby, largeBatch);
        assertThat(mapActive.size()).isEqualTo(largeBatch);
        log.info("After role switch during apply phase, both maps have size {}. Current " +
                        "active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                mapActive.size(), activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(largeBatch);
        assertThat(mapStandby.size()).isEqualTo(largeBatch);
    }

    /**
     * This test verifies config change with two active clusters
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a two-active config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since both are active
     */
    @Test
    public void testNewConfigWithTwoActive() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with two active
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_TWO_ACTIVE,
                        DefaultClusterManager.OP_TWO_ACTIVE, DefaultClusterManager.OP_TWO_ACTIVE)
                .commit();
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        log.info("Active map has {} entries now!", thirdBatch);

        // Standby map should still have secondBatch size
        log.info("Standby map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with two standby clusters
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a two-standby config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since both are standby
     */
    @Test
    public void testNewConfigWithAllStandby() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with all standby
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_ALL_STANDBY,
                        DefaultClusterManager.OP_ALL_STANDBY, DefaultClusterManager.OP_ALL_STANDBY)
                .commit();
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        for (int i = secondBatch; i < thirdBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        log.info("Active map has {} entries now!", thirdBatch);

        // Standby map should still have secondBatch size
        log.info("Standby map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with one active and one invalid
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a active-invalid config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since standby is invalid
     * 9. Resume to standby and verify data is fully replicated again.
     */
    @Test
    public void testNewConfigWithInvalidClusters() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with invalid state
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_INVALID,
                        DefaultClusterManager.OP_INVALID, DefaultClusterManager.OP_INVALID)
                .commit();
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(thirdBatch);

        // Standby map should still have secondBatch size
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(secondBatch);
        log.info("After {} seconds sleep, double check passed", mediumInterval);

        // Change to default active standby config
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_RESUME,
                        DefaultClusterManager.OP_RESUME, DefaultClusterManager.OP_RESUME)
                .commit();
        assertThat(configTable.count()).isEqualTo(2);
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Standby map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapStandby, thirdBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.size()).isEqualTo(thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(thirdBatch);
    }

    private Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> getMetadataTable(CorfuRuntime runtime) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        CorfuStore corfuStore = new CorfuStore(runtime);
        CorfuStoreMetadata.TableName metadataTableName = null;
        Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable = null;

        for (CorfuStoreMetadata.TableName name : corfuStore.listTables(LogReplicationMetadataManager.NAMESPACE)){
            if(name.getTableName().contains(LogReplicationMetadataManager.METADATA_TABLE_PREFIX_NAME)) {
                metadataTableName = name;
            }
        }

        metadataTable = corfuStore.openTable(
                    LogReplicationMetadataManager.NAMESPACE,
                    metadataTableName.getTableName(),
                    LogReplicationMetadataKey.class,
                    LogReplicationMetadataVal.class,
                    null,
                    TableOptions.builder().build());

        return metadataTable;
    }


    /**
     * This test verifies enforceSnapshotSync API
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Write 5 more entries to active map and perform an enforced full snapshot sync
     * 7. Verify a full snapshot sync is triggered
     * 8. Verify a full snapshot sync is completed and data is correctly replicated.
     */
    @Test
    public void testEnforceSnapshotSync() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);
        assertThat(mapStandby.size()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");


        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);

        Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable = getMetadataTable(standbyRuntime);
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(LAST_SNAPSHOT_APPLIED.getVal()).build();
        long lastAppliedSnapshot = Long.parseLong(metadataTable.get(txKey).getPayload().getVal());

        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {} lastAppliedSnapshot {}",
                firstBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail(), lastAppliedSnapshot);
        assertThat(lastAppliedSnapshot).isEqualTo(activeRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }

        log.info("Log replication succeeds before enforcing full snapshot sync! LastSnapshotApplied Timestamp {}",
                Long.parseLong(metadataTable.get(txKey).getPayload().getVal()));

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(thirdBatch);

        // Perform an enforce full snapshot sync
        corfuStore.tx(DefaultClusterManager.CONFIG_NAMESPACE)
                .update(DefaultClusterManager.CONFIG_TABLE_NAME, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                        DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC)
                .commit();
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Verify that a fullsnapshot sync is triggered.
        txKey = LogReplicationMetadataKey.newBuilder().setKey(LAST_SNAPSHOT_STARTED.getVal()).build();
        long newSnapshotTimestamp = Long.parseLong(metadataTable.get(txKey).getPayload().getVal());
        assertThat(lastAppliedSnapshot).isLessThan(newSnapshotTimestamp);
        assertThat(newSnapshotTimestamp).isEqualTo(activeRuntime.getAddressSpaceView().getLogTail());

        // Standby map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapStandby, thirdBatch);
        assertThat(mapStandby.size()).isEqualTo(thirdBatch);

        txKey = LogReplicationMetadataKey.newBuilder().setKey(LAST_SNAPSHOT_APPLIED.getVal()).build();
        log.info("NewSnapshot Timestamp {} active logTail {} after full snapshot sync!", newSnapshotTimestamp, activeRuntime.getAddressSpaceView().getLogTail());

        // The enforced snapshot sync completed.
        assertThat(newSnapshotTimestamp).isEqualTo(Long.parseLong(metadataTable.get(txKey).getPayload().getVal()));
    }

    private void waitForReplication(IntPredicate verifier, CorfuTable table, int expected) {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            log.info("Waiting for replication, table size is {}, expected size is {}", table.size(), expected);
            if (verifier.test(table.size())) {
                break;
            }
            sleepUninterruptibly(shortInterval);
        }
        assertThat(verifier.test(table.size())).isTrue();
    }


    private void sleepUninterruptibly(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }
}