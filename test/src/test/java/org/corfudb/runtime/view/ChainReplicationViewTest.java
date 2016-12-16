package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 12/25/15.
 */
public class ChainReplicationViewTest extends AbstractViewTest {


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingleConcurrent()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();

        final int numberThreads = 5;
        final int numberRecords = 1_000;

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                r.getAddressSpaceView().write(i, Collections.singleton(CorfuRuntime.getStreamID("a")),
                        Integer.toString(i).getBytes(), Collections.emptyMap(), Collections.emptyMap());
            }
        });
        executeScheduled(numberThreads, PARAMETERS.TIMEOUT_LONG);

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                assertThat(r.getAddressSpaceView().read(i).getPayload(getRuntime()))
                        .isEqualTo(Integer.toString(i).getBytes());
            }
        });
        executeScheduled(numberThreads, PARAMETERS.TIMEOUT_LONG);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToMultiple()
            throws Exception {

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(SERVERS.PORT_0)
                        .addLogUnit(SERVERS.PORT_1)
                        .addLogUnit(SERVERS.PORT_2)
                    .addToSegment()
                .addToLayout()
                .build());


        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();


        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureAllUnitsContainData()
            throws Exception {

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(SERVERS.PORT_0)
                        .addLogUnit(SERVERS.PORT_1)
                        .addLogUnit(SERVERS.PORT_2)
                    .addToSegment()
                .addToLayout()
                .build());

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(SERVERS.PORT_1))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(SERVERS.PORT_2))
                .matchesDataAtAddress(0, testPayload);
    }
}
