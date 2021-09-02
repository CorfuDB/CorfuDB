package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


import com.google.common.cache.Cache;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.corfudb.common.compression.Codec;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

/**
 * Created by mwei on 2/1/16.
 */
public class AddressSpaceViewTest extends AbstractViewTest {

    private void setupNodes() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        // configure the layout accordingly
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
    }

    @Test
    public void incorrectCacheSetting() {
        setupNodes();
        final int oneMb = 1_000_000;

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxCacheWeight(oneMb)
                .maxCacheEntries(oneMb)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        assertThatThrownBy(() -> rt.getAddressSpaceView())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void checkpointEntryNotCached() {
        setupNodes();
        final int oneMb = 1_000_000;

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxCacheEntries(oneMb)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        CorfuRuntime checkpointRt = CorfuRuntime.fromParameters(params)
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        AddressSpaceView addressSpaceView = rt.getAddressSpaceView();

        // write a checkpoint entry by checkpoint runtime
        String streamName = "test-stream";
        UUID streamId = CorfuRuntime.getStreamID(streamName);
        UUID checkpointId = UUID.randomUUID();
        IStreamView sv = checkpointRt.getStreamsView().get(streamId);
        long address = checkpointRt.getSequencerView().query(streamId);
        Map<CheckpointEntry.CheckpointDictKey, String> mdKV = new HashMap<>();
        mdKV.put(CheckpointEntry.CheckpointDictKey.START_TIME, "The perfect time");
        mdKV.put(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS, Long.toString(address + 1));
        CheckpointEntry cp1 = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                "checkpointAuthor", checkpointId, streamId, mdKV, null);
        long cpAddress = sv.append(cp1, null, null);

        // read the cp entry
        ILogData ldRead = addressSpaceView.read(cpAddress);
        assertThat(ldRead).isNotNull();
        ILogData ldCache = addressSpaceView.getReadCache().getIfPresent(cpAddress);
        assertThat(ldCache).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingWorks() throws Exception {
        setupNodes();
        CorfuRuntime rt = getRuntime().connect();
        rt.getParameters().setCodecType(Codec.Type.NONE);

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        final long epoch = rt.getLayoutView().getLayout().getEpoch();

        rt.getAddressSpaceView().write(new TokenResponse(new Token(epoch, 0),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                "hello world".getBytes());

        assertThat(rt.getAddressSpaceView().read(0L).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(rt.getAddressSpaceView().read(0L).containsStream(streamA))
                .isTrue();

        // Ensure that the data was written to each logunit.
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_0))
                .matchesDataAtAddress(0, testPayload);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_1))
                .isEmptyAtAddress(0);
        LogUnitServerAssertions.assertThat(getLogUnit(SERVERS.PORT_2))
                .isEmptyAtAddress(0);

        rt.getAddressSpaceView().write(new TokenResponse(new Token(epoch, 1),
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
    public void testUncachedWrites() {
        setupNodes();
        CorfuRuntime rt = getRuntime().connect();

        final long epoch = rt.getLayoutView().getLayout().getEpoch();

        // Write two entries, with different cache options
        rt.getAddressSpaceView().write(new TokenResponse(new Token(epoch, 0),
                Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes(), CacheOption.WRITE_THROUGH);

        rt.getAddressSpaceView().write(new TokenResponse(new Token(epoch, 1),
                        Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes(), CacheOption.WRITE_AROUND);

        // write with the default write method
        rt.getAddressSpaceView().write(new TokenResponse(new Token(epoch, 2),
                        Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes());

        // Verify that write to address 0 is cached and that the write to address 1 isn't cached
        Cache<Long, ILogData> clientCache = rt.getAddressSpaceView().getReadCache();

        assertThat(clientCache.getIfPresent(0L)).isNotNull();
        assertThat(clientCache.getIfPresent(1L)).isNull();
        // Since the default behavior of write is to cache entries, address 2 should
        // be cached
        assertThat(clientCache.getIfPresent(2L)).isNotNull();
    }

    @Test
    public void testGetTrimMark() {
        setupNodes();
        CorfuRuntime rt = getRuntime().connect();
        assertThat(rt.getAddressSpaceView().getTrimMark().getSequence()).isEqualTo(0);
        final Token trimAddress = new Token(rt.getLayoutView().getLayout().getEpoch(), 10);

        rt.getAddressSpaceView().prefixTrim(trimAddress);
        assertThat(rt.getAddressSpaceView().getTrimMark().getSequence()).isEqualTo(trimAddress.getSequence() + 1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingReadAllWorks() throws Exception {
        setupNodes();
        CorfuRuntime rt = getRuntime().connect();

        byte[] testPayload = "hello world".getBytes();

        final long ADDRESS_0 = 0;
        final long ADDRESS_1 = 1;
        final long ADDRESS_2 = 3;
        Token token = new Token(rt.getLayoutView().getLayout().getEpoch(), ADDRESS_0);
        rt.getAddressSpaceView().write(token, testPayload);

        assertThat(rt.getAddressSpaceView().read(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());


        rt.getAddressSpaceView().write(new Token(rt.getLayoutView().getLayout().getEpoch(), ADDRESS_1),
                "1".getBytes());

        rt.getAddressSpaceView().write(new Token(rt.getLayoutView().getLayout().getEpoch(), ADDRESS_2),
                "3".getBytes());

        List<Long> rs = new ArrayList<>();
        rs.add(ADDRESS_0);
        rs.add(ADDRESS_1);
        rs.add(ADDRESS_2);

        Map<Long, ILogData> m = rt.getAddressSpaceView().read(rs);

        assertThat(m.get(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(ADDRESS_1).getPayload(getRuntime()))
                .isEqualTo("1".getBytes());
        assertThat(m.get(ADDRESS_2).getPayload(getRuntime()))
                .isEqualTo("3".getBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readAllWithHoleFill() throws Exception {
        setupNodes();
        CorfuRuntime rt = getRuntime().connect();

        byte[] testPayload = "hello world".getBytes();

        final long ADDRESS_0 = 0;
        final long ADDRESS_1 = 1;
        final long ADDRESS_2 = 3;
        Token token = new Token(rt.getLayoutView().getLayout().getEpoch(), ADDRESS_0);
        rt.getAddressSpaceView().write(token, testPayload);

        assertThat(rt.getAddressSpaceView().read(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        Range range = Range.closed(ADDRESS_0, ADDRESS_2);
        ContiguousSet<Long> addresses = ContiguousSet.create(range, DiscreteDomain.longs());

        Map<Long, ILogData> m = rt.getAddressSpaceView().read(addresses);

        assertThat(m.get(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(ADDRESS_1).isHole()).isTrue();
        assertThat(m.get(ADDRESS_2).isHole()).isTrue();
    }

    /**
     * Test bulk read can query the correct log unit server in case
     * the requested addresses are stripped and span segments.
     */
    @Test
    public void testMultiReadSpansSegments() {
        final long segmentEnd = 5L;
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setStart(0L)
                .setEnd(segmentEnd)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(segmentEnd)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build());

        // Shutdown management server to prevent segment merge
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        CorfuRuntime rt = getRuntime();
        rt.setCacheDisabled(true);
        rt.connect();

        final String testString = "hello world ";

        final long numAddresses = 10L;
        for (long i = 0L; i < numAddresses; i++) {
            TokenResponse token = rt.getSequencerView().next();
            rt.getAddressSpaceView().write(token, (testString + i).getBytes());
        }

        Map<Long, ILogData> readResult = rt.getAddressSpaceView().read(
                ContiguousSet.create(Range.closed(0L, numAddresses - 1), DiscreteDomain.longs()));

        readResult.forEach((addr, data) ->
                assertThat(data.getPayload(rt)).isEqualTo((testString + addr).getBytes()));
    }
}