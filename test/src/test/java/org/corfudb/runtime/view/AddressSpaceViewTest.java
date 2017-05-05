package org.corfudb.runtime.view;

import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/1/16.
 */
public class AddressSpaceViewTest extends AbstractViewTest {

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingWorks()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        //configure the layout accordingly
        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        final long epoch = r.getLayoutView().getLayout().getEpoch();

        r.getAddressSpaceView().write(new TokenResponse(0, epoch,
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                "hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(r.getAddressSpaceView().read(0L).containsStream(streamA))
                .isTrue();

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_1))
                .isEmptyAtAddress(0);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_2))
                .isEmptyAtAddress(0);

        r.getAddressSpaceView().write(new TokenResponse(1, epoch,
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                "1".getBytes());
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_1))
                .matchesDataAtAddress(1, "1".getBytes());
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_2))
                .isEmptyAtAddress(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingReadAllWorks()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        final long ADDRESS_0 = 0;
        final long ADDRESS_1 = 1;
        final long ADDRESS_2 = 3;
        Token token = new Token(ADDRESS_0, r.getLayoutView().getLayout().getEpoch());
        r.getAddressSpaceView().write(token, testPayload);

        assertThat(r.getAddressSpaceView().read(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());


        r.getAddressSpaceView().write(new Token(ADDRESS_1, r.getLayoutView().getLayout().getEpoch()),
                "1".getBytes());

        r.getAddressSpaceView().write(new Token(ADDRESS_2, r.getLayoutView().getLayout().getEpoch()),
                "3".getBytes());

        List<Long> rs = new ArrayList<>();
        rs.add(ADDRESS_0);
        rs.add(ADDRESS_1);
        rs.add(ADDRESS_2);

        Map<Long, ILogData> m = r.getAddressSpaceView().read(rs);

        assertThat(m.get(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(ADDRESS_1).getPayload(getRuntime()))
                .isEqualTo("1".getBytes());
        assertThat(m.get(ADDRESS_2).getPayload(getRuntime()))
                .isEqualTo("3".getBytes());
    }
}