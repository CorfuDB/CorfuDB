package org.corfudb.runtime.view;

import com.google.common.collect.*;
import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
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

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void cacheMissTimesOut() {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        getRuntime().setCacheDisabled(false).connect();

        getRuntime().getAddressSpaceView().setEmptyDuration(Duration.ofNanos(100));
        assertThat(getRuntime().getAddressSpaceView().read(0).getResult().getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.EMPTY);
        getRuntime().getLayoutView().getLayout().getLogUnitClient(0, 0).fillHole(0);
        try {Thread.sleep(100);} catch (InterruptedException e) {// don't do anything
        }
        assertThat(getRuntime().getAddressSpaceView().read(0).getResult().getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.FILLED_HOLE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingWorks()
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
                        ImmutableList.<Layout.LayoutStripe>builder()
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9000))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9001))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9002))))
                                .build()
                )),
                1L
        ));

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getResult().getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>)r.getAddressSpaceView().read(0L).getResult().getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(l9000)
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(l9001)
                .isEmptyAtAddress(0);
        LogUnitServerAssertions.assertThat(l9002)
                .isEmptyAtAddress(0);

        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap());
        LogUnitServerAssertions.assertThat(l9000)
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(l9001)
                .matchesDataAtAddress(0, "1".getBytes());
        LogUnitServerAssertions.assertThat(l9002)
                .isEmptyAtAddress(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingReadAllWorks()
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
                        ImmutableList.<Layout.LayoutStripe>builder()
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9000))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9001))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9002))))
                                .build()
                )),
                1L
        ));

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getResult().getPayload(r))
                .isEqualTo("hello world".getBytes());


        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().write(3, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap());

        RangeSet<Long> rs = TreeRangeSet.create();
        rs.add(Range.closed(0L, 3L));
        Map<Long, AbstractReplicationView.ReadResult> m = r.getAddressSpaceView().read(rs);

        assertThat(m.get(0L).getResult().getPayload(null))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(1L).getResult().getPayload(null))
                .isEqualTo("1".getBytes());
        assertThat(m.get(3L).getResult().getPayload(null))
                .isEqualTo("3".getBytes());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingStreamReadAllWorks()
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
                        ImmutableList.<Layout.LayoutStripe>builder()
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9000))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9001))))
                                .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9002))))
                                .build()
                )),
                1L
        ));

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());


        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().write(2, Collections.singleton(streamB),
                "2".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().write(3, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().write(5, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().compactAll();

       Map<Long, AbstractReplicationView.ReadResult>  aAddresses = r.getAddressSpaceView().readPrefix(streamA);
        assertThat(aAddresses.keySet())
                .contains(0L)
                .contains(1L)
                .contains(3L)
                .doesNotContain(2L);
    }
}
