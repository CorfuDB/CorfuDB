package org.corfudb.integration;

import org.corfudb.compactor.CorfuStoreCompactor;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.test.SampleSchema.Uuid;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing CorfuStoreCompactor.
 * Created by zlokhandwala on 11/6/19.
 */
public class CorfuStoreCompactorIT extends AbstractIT {

    private Process server;
    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() throws IOException {
        String corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        int corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));
        String singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);

        server = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        corfuRuntime = createRuntime(singleNodeEndpoint);
        corfuStore = new CorfuStore(corfuRuntime);
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        corfuRuntime.shutdown();
        assertThat(shutdownCorfuServer(server)).isTrue();
    }


    private Uuid getUuid(UUID uuid) {
        return Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();
    }

    /**
     * Performa a compaction and verifies the trim mark.
     */
    @Test
    public void compactData() throws Exception {
        String namespace = "namespace";
        String tableName = "tableName";
        corfuStore.openTable(namespace,
                tableName,
                Uuid.class,
                Uuid.class,
                Uuid.class,
                TableOptions.builder().build());

        Token firstTrimMark = corfuRuntime.getAddressSpaceView().getTrimMark();
        assertThat(firstTrimMark)
                .isEqualTo(new Token(0, 0));
        final int updatesNum = 10;

        for (int i = 0; i < updatesNum; i++) {
            corfuStore.tx(namespace)
                    .update(tableName,
                            getUuid(UUID.nameUUIDFromBytes("key".getBytes())),
                            getUuid(UUID.nameUUIDFromBytes("val".getBytes())),
                            getUuid(UUID.nameUUIDFromBytes("meta".getBytes())))
                    .commit();
        }

        CorfuStoreCompactor compactor = new CorfuStoreCompactor(corfuRuntime, Duration.ZERO);
        final int compactionCycles = 3;
        for (int i = 0; i < compactionCycles; i++) {
            compactor.compact();
        }

        /*
         * Data:
         * 0: Registry table schemas persisted.
         * 1: New table schemas persisted.
         * [2 - 11): 10 updates.
         * 12, 13: Initialization of Checkpoint and Trim Tables.
         */
        final long lastUpdatedAddress = 13;
        Token expectedTrimMark = new Token(0, lastUpdatedAddress);
        Token newTrimMark = corfuRuntime.getAddressSpaceView().getTrimMark();
        assertThat(newTrimMark)
                .isEqualTo(expectedTrimMark);
    }
}
