package org.corfudb.runtime.view;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.corfudb.infrastructure.*;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/1/16.
 */
public class AddressSpaceViewTest extends AbstractViewTest {

    /** This test checks to see if stale cache misses time out,
     *  That is, if a cache read returns EMPTY, after the timeout period
     *  the cache should retry the read, and get the correct result.
     *
     *  In this case, the cache miss initially misses, but we fill a hole.
     *  After the timeout, the client should see the hole, rather than
     *  empty again.
     */
    @Test
    public void cacheMissTimesOut() {
        getDefaultRuntime().setCacheDisabled(false).connect();

        getRuntime().getAddressSpaceView().setEmptyDuration(PARAMETERS.TIMEOUT_VERY_SHORT);
        assertThat(getRuntime().getAddressSpaceView().read(0).getType())
                .isEqualTo(DataType.EMPTY);
        getRuntime().getLayoutView().getLayout().getLogUnitClient(0, 0).fillHole(0);
        try {
            Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());
        } catch (InterruptedException e) {// don't do anything
        }
        assertThat(getRuntime().getAddressSpaceView().read(0).getType())
                .isEqualTo(DataType.HOLE);
    }

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

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_1))
                .isEmptyAtAddress(0);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_2))
                .isEmptyAtAddress(0);

        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap(), Collections.emptyMap());
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
        r.getAddressSpaceView().write(ADDRESS_0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());


        r.getAddressSpaceView().write(ADDRESS_1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap(), Collections.emptyMap());

        r.getAddressSpaceView().write(ADDRESS_2, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap(), Collections.emptyMap());

        RangeSet<Long> rs = TreeRangeSet.create();
        rs.add(Range.closed(0L, ADDRESS_2));
        Map<Long, ILogData> m = r.getAddressSpaceView().read(rs);

        assertThat(m.get(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(ADDRESS_1).getPayload(getRuntime()))
                .isEqualTo("1".getBytes());
        assertThat(m.get(ADDRESS_2).getPayload(getRuntime()))
                .isEqualTo("3".getBytes());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingStreamReadAllWorks()
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
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());
        byte[] testPayload = "hello world".getBytes();

        final int ADDRESS_0 = 0;
        final int ADDRESS_1 = 1;
        final int ADDRESS_2 = 2;
        final int ADDRESS_3 = 3;
        final int ADDRESS_4 = 5;

        r.getAddressSpaceView().write(ADDRESS_0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap(), Collections.emptyMap());


        r.getAddressSpaceView().write(ADDRESS_1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap(), Collections.emptyMap());

        r.getAddressSpaceView().write(ADDRESS_2, Collections.singleton(streamB),
                "2".getBytes(), Collections.emptyMap(), Collections.emptyMap());

        r.getAddressSpaceView().write(ADDRESS_3, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap(), Collections.emptyMap());

        r.getAddressSpaceView().write(ADDRESS_4, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap(), Collections.emptyMap());

    }
}
