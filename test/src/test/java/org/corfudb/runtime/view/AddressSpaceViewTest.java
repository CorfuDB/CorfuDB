package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/1/16.
 */
public class AddressSpaceViewTest extends AbstractViewTest {

    @Before
    public void setup(){
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
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingWorks()
            throws Exception {
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
    public void testUncachedWrites() {
        CorfuRuntime r = getRuntime().connect();

        final long epoch = r.getLayoutView().getLayout().getEpoch();

        // Write two entries, with different cache options
        r.getAddressSpaceView().write(new TokenResponse(0, epoch,
                Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes(), CacheOption.WRITE_THROUGH);

        r.getAddressSpaceView().write(new TokenResponse(1, epoch,
                        Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes(), CacheOption.WRITE_AROUND);

        // write with the default write method
        r.getAddressSpaceView().write(new TokenResponse(2, epoch,
                        Collections.singletonMap(CorfuRuntime.getStreamID("stream1"), Address.NO_BACKPOINTER)),
                "payload".getBytes());

        // Verify that write to address 0 is cached and that the write to address 1 isn't cached

        LoadingCache<Long, ILogData> clientCache = r.getAddressSpaceView().getReadCache();

        assertThat(clientCache.getIfPresent(0L)).isNotNull();
        assertThat(clientCache.getIfPresent(1L)).isNull();
        // Since the default behavior of write is to cache entries, address 2 should
        // be cached
        assertThat(clientCache.getIfPresent(2L)).isNotNull();
    }

    @Test
    public void testGetTrimMark() {
        CorfuRuntime r = getRuntime().connect();
        assertThat(r.getAddressSpaceView().getTrimMark()).isEqualTo(0);
        final long trimAddress = 10;

        r.getAddressSpaceView().prefixTrim(trimAddress);
        assertThat(r.getAddressSpaceView().getTrimMark()).isEqualTo(trimAddress + 1);
    }

    /**
     * This test verifies that the trimMark is synced correctly by the CorfuRuntime.
     * After performing 10 writes, the runtime trims on address 3.
     * Finally the trimMarkSync is verified by reading the addressSpaceView cache and asserting
     * that the 3 trimmed entries are removed from the cache.
     */
    @Test
    public void testSyncTrimMark() {
        CorfuRuntime r = getRuntime().connect();
        AddressSpaceView spaceView = r.getAddressSpaceView();
        assertThat(spaceView.getTrimMark()).isEqualTo(0);

        final String basicTestPayload = "payload";
        final long layoutEpoch = r.getLayoutView().getLayout().getEpoch();
        final int entryNum = 10;

        for (int i = 0; i < entryNum; i++) {
            String payload = basicTestPayload + String.valueOf(i);
            spaceView.write(new Token(i, layoutEpoch), payload.getBytes());
        }

        final int inclusiveTrimAddress = 3;
        spaceView.prefixTrim(inclusiveTrimAddress);
        final int trimMark = (int) spaceView.getTrimMark();
        assertThat(trimMark).isEqualTo(inclusiveTrimAddress + 1);
        assertThat(spaceView.getReadCache().asMap().size()).isEqualTo(entryNum);

        // Run TrimMarkSyncTask once.
        AddressSpaceView.TrimMarkSyncTask trimMarkSyncTask = spaceView.new TrimMarkSyncTask();
        trimMarkSyncTask.run();

        // Keys less than trimMark should be removed from cache.
        int expectedCacheSize = entryNum - trimMark;
        assertThat(spaceView.getReadCache().asMap().size()).isEqualTo(expectedCacheSize);
        assertThat(Collections.min(spaceView.getReadCache().asMap().keySet())).isEqualTo(trimMark);

        // After shutdown, cache has been cleared.
        spaceView.shutdown();
        assertThat(spaceView.getReadCache().asMap().isEmpty()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void ensureStripingReadAllWorks()
            throws Exception {
        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

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

    @Test
    @SuppressWarnings("unchecked")
    public void readAllWithHoleFill()
            throws Exception {
        //configure the layout accordingly
        CorfuRuntime r = getRuntime().connect();

        byte[] testPayload = "hello world".getBytes();

        final long ADDRESS_0 = 0;
        final long ADDRESS_1 = 1;
        final long ADDRESS_2 = 3;
        Token token = new Token(ADDRESS_0, r.getLayoutView().getLayout().getEpoch());
        r.getAddressSpaceView().write(token, testPayload);

        assertThat(r.getAddressSpaceView().read(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        Range range = Range.closed(ADDRESS_0, ADDRESS_2);
        ContiguousSet<Long> addresses = ContiguousSet.create(range, DiscreteDomain.longs());

        Map<Long, ILogData> m = r.getAddressSpaceView().read(addresses);

        assertThat(m.get(ADDRESS_0).getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(ADDRESS_1).isHole());
        assertThat(m.get(ADDRESS_2).isHole());
    }
}