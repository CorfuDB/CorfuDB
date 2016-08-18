package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
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
                testPayload, Collections.emptyMap());

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
        final int numberRecords = 10_000;

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                r.getAddressSpaceView().write(i, Collections.singleton(CorfuRuntime.getStreamID("a")),
                        Integer.toString(i).getBytes(), Collections.emptyMap());
            }
        });
        executeScheduled(numberThreads, 50, TimeUnit.SECONDS);

        scheduleConcurrently(numberThreads, threadNumber -> {
            int base = threadNumber * numberRecords;
            for (int i = base; i < base + numberRecords; i++) {
                assertThat(r.getAddressSpaceView().read(i).getPayload(getRuntime()))
                        .isEqualTo(Integer.toString(i).getBytes());
            }
        });
        executeScheduled(numberThreads, 50, TimeUnit.SECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToMultiple()
            throws Exception {

        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(9000)
                        .addLogUnit(9001)
                        .addLogUnit(9002)
                    .addToSegment()
                .addToLayout()
                .build());


        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();


        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());

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

        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                    .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                    .buildStripe()
                        .addLogUnit(9000)
                        .addLogUnit(9001)
                        .addLogUnit(9002)
                    .addToSegment()
                .addToLayout()
                .build());

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        assertThat(getLogUnit(9000))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(9001))
                .matchesDataAtAddress(0, testPayload);
        assertThat(getLogUnit(9002))
                .matchesDataAtAddress(0, testPayload);
    }
}
