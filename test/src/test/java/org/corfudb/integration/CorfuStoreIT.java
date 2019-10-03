package org.corfudb.integration;

import org.corfudb.runtime.collections.*;
import org.corfudb.test.CorfuServerRunner;
import org.corfudb.test.SampleSchema;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.test.CorfuServerRunner.getCorfuServerLogPath;

/**
 * Simple test that inserts data into CorfuStore via a separate server process
 */
public class CorfuStoreIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /** Load properties for a single node corfu server before each test*/
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * Basic test that inserts a single item using protobuf defined in the proto/ directory.
     * @throws Exception
     */
    @Test
    public void testTx() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                "namespace", "table", SampleSchema.Uuid.class,
                SampleSchema.Uuid.class, SampleSchema.Uuid.class,
                TableOptions.builder().build()
        );
        UUID keyUuid = UUID.randomUUID();
        UUID valueUuid = UUID.randomUUID();
        UUID metadataUuid = UUID.randomUUID();
        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid.getMostSignificantBits())
                .setLsb(keyUuid.getLeastSignificantBits())
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid.getMostSignificantBits())
                .setLsb(valueUuid.getLeastSignificantBits())
                .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
                .setMsb(metadataUuid.getMostSignificantBits())
                .setLsb(metadataUuid.getLeastSignificantBits())
                .build();
        TxBuilder tx = store.tx("namespace");
        tx.create("table", uuidKey, uuidVal,metadata)
                .update("table", uuidKey, uuidVal, metadata)
                .commit();
        CorfuRecord record = table.get(uuidKey);
        assertThat(record.getPayload()).isEqualTo(uuidVal);
        assertThat(record.getMetadata()).isEqualTo(metadata);
    }
}
