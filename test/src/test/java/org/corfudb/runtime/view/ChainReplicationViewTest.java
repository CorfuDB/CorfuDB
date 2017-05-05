package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

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

        r.getAddressSpaceView().write(new TokenResponse(0,
                        runtime.getLayoutView().getLayout().getEpoch(),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).containsStream(streamA))
                .isTrue();
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
                r.getAddressSpaceView().write(new TokenResponse((long)i,
                                runtime.getLayoutView().getLayout().getEpoch(),
                                Collections.singletonMap(CorfuRuntime.getStreamID("a"), Address.NO_BACKPOINTER)),
                        Integer.toString(i).getBytes());
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

        r.getAddressSpaceView().write(new TokenResponse(0,
                        runtime.getLayoutView().getLayout().getEpoch(),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L)
                .containsStream(streamA)).isTrue();
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

        r.getAddressSpaceView().write(new TokenResponse(0, 0,
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).containsStream(streamA));

        // Ensure that the data was written to each logunit.
        assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(SERVERS.PORT_1))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(SERVERS.PORT_2))
                .matchesDataAtAddress(0, testPayload);
    }
}
