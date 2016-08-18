package org.corfudb.runtime.view;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.corfudb.infrastructure.*;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
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

    @Test
    public void cacheMissTimesOut() {
        getDefaultRuntime().setCacheDisabled(false).connect();

        getRuntime().getAddressSpaceView().setEmptyDuration(Duration.ofNanos(100));
        assertThat(getRuntime().getAddressSpaceView().read(0).getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.EMPTY);
        getRuntime().getLayoutView().getLayout().getLogUnitClient(0, 0).fillHole(0);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {// don't do anything
        }
        assertThat(getRuntime().getAddressSpaceView().read(0).getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.FILLED_HOLE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingWorks()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);

        //configure the layout accordingly
        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                    .buildSegment()
                        .buildStripe()
                            .addLogUnit(9000)
                            .addToSegment()
                        .buildStripe()
                            .addLogUnit(9001)
                            .addToSegment()
                        .buildStripe()
                            .addLogUnit(9002)
                            .addToSegment()
                    .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        r.getAddressSpaceView().write(0, Collections.singleton(streamA),
                testPayload, Collections.emptyMap());

        assertThat(r.getAddressSpaceView().read(0L).getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat((Set<UUID>) r.getAddressSpaceView().read(0L).getMetadataMap()
                .get(IMetadata.LogUnitMetadataType.STREAM))
                .contains(streamA);

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(getLogUnit(9000))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(9001))
                .isEmptyAtAddress(0);
        LogUnitServerAssertions.assertThat(getLogUnit(9002))
                .isEmptyAtAddress(0);

        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap());
        LogUnitServerAssertions.assertThat(getLogUnit(9000))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(9001))
                .matchesDataAtAddress(0, "1".getBytes());
        LogUnitServerAssertions.assertThat(getLogUnit(9002))
                .isEmptyAtAddress(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingReadAllWorks()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                    .buildSegment()
                        .buildStripe()
                            .addLogUnit(9000)
                            .addToSegment()
                        .buildStripe()
                            .addLogUnit(9001)
                            .addToSegment()
                        .buildStripe()
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

        assertThat(r.getAddressSpaceView().read(0L).getPayload())
                .isEqualTo("hello world".getBytes());


        r.getAddressSpaceView().write(1, Collections.singleton(streamA),
                "1".getBytes(), Collections.emptyMap());

        r.getAddressSpaceView().write(3, Collections.singleton(streamA),
                "3".getBytes(), Collections.emptyMap());

        RangeSet<Long> rs = TreeRangeSet.create();
        rs.add(Range.closed(0L, 3L));
        Map<Long, ILogUnitEntry> m = r.getAddressSpaceView().read(rs);

        assertThat(m.get(0L).getPayload())
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(1L).getPayload())
                .isEqualTo("1".getBytes());
        assertThat(m.get(3L).getPayload())
                .isEqualTo("3".getBytes());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingStreamReadAllWorks()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                    .buildSegment()
                        .buildStripe()
                            .addLogUnit(9000)
                            .addToSegment()
                        .buildStripe()
                            .addLogUnit(9001)
                            .addToSegment()
                        .buildStripe()
                            .addLogUnit(9002)
                            .addToSegment()
                    .addToLayout()
                .build());

        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

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

    }
}
