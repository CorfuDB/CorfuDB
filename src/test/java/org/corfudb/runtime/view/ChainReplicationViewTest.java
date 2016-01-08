package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 12/25/15.
 */
public class ChainReplicationViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
    throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA), testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>)r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToMultiple()
    throws Exception
    {
        // default layout is chain replication.
        addServerForTest(getEndpoint(9000), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getEndpoint(9000), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getEndpoint(9001), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getEndpoint(9002), new LogUnitServer(defaultOptionsMap()));
        wireRouters();

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();
        setLayout(new Layout(
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        ImmutableList.<String>builder()
                            .add(getEndpoint(9000))
                            .add(getEndpoint(9001))
                            .add(getEndpoint(9002))
                        .build()
                )),
                1L
        ));

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA), testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>)r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureAllUnitsContainData()
            throws Exception
    {
        // default layout is chain replication.
        addServerForTest(getEndpoint(9000), new LayoutServer(defaultOptionsMap()));

        LogUnitServer l9000 = new LogUnitServer(defaultOptionsMap());
        LogUnitServer l9001 = new LogUnitServer(defaultOptionsMap());
        LogUnitServer l9002 = new LogUnitServer(defaultOptionsMap());

        addServerForTest(getEndpoint(9000), l9000);
        addServerForTest(getEndpoint(9001), l9001);
        addServerForTest(getEndpoint(9002), l9002);
        wireRouters();

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();
        setLayout(new Layout(
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        ImmutableList.<String>builder()
                                .add(getEndpoint(9000))
                                .add(getEndpoint(9001))
                                .add(getEndpoint(9002))
                                .build()
                )),
                1L
        ));

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA), testPayload);

        assertThat(r.getAddressSpaceView().read(0L).getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>)r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        assertThat(l9000)
            .matchesDataAtAddress(0, testPayload);
        assertThat(l9001)
            .matchesDataAtAddress(0, testPayload);
        assertThat(l9002)
            .matchesDataAtAddress(0, testPayload);
    }
}
