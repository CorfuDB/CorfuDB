package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getDataCorruptionErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getOverwriteErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getTrimmedErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCommittedTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCompactResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getFlushCacheResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getInspectAddressesResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getKnownAddressResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getLogAddressSpaceResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getRangeWriteLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getResetLogUnitResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimMarkResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getUpdateCommittedTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import static org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class LogUnitHandlerTest {

    // The LogUnitHandler instance used for testing.
    private LogUnitHandler logUnitHandler;

    // Objects that need to be mocked.
    private IClientRouter mockClientRouter;
    private ChannelHandlerContext mockChannelHandlerContext;

    private final AtomicInteger requestCounter = new AtomicInteger();

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * Perform the required preparation before running individual
     * tests by preparing the mocks.
     */
    @Before
    public void setup() {
        mockClientRouter = mock(IClientRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        logUnitHandler = new LogUnitHandler();
        logUnitHandler.setRouter(mockClientRouter);
    }


    /**
     * Test that the LogUnitHandler correctly handles a WRITE_LOG_RESPONSE.
     */
    @Test
    public void testWrite() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getWriteLogResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a RANGE_WRITE_LOG_RESPONSE.
     */
    @Test
    public void testWriteRange() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getRangeWriteLogResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a READ_LOG_RESPONSE.
     */
    @Test
    public void testRead() {
        ReadResponse rr = new ReadResponse();
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getReadLogResponseMsg(rr.getAddresses())
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), rr);
    }

    /**
     * Test that the LogUnitHandler correctly handles a INSPECT_ADDRESSES_RESPONSE.
     */
    @Test
    public void testInspectAddresses() {
        List<Long> emptyAddresses = new ArrayList<>();
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getInspectAddressesResponseMsg(emptyAddresses)
        );

        ArgumentCaptor<InspectAddressesResponse> captor = ArgumentCaptor.forClass(InspectAddressesResponse.class);

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), captor.capture());
        assertEquals(emptyAddresses, captor.getValue().getEmptyAddresses());
    }

    /**
     * Test that the LogUnitHandler correctly handles a TAIL_RESPONSE for LOG_TAIL request.
     */
    @Test
    public void testTailResponse() {
        TailsResponse sampleTailsResponse = new TailsResponse(0L, 0L, new HashMap<>());
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getTailResponseMsg(sampleTailsResponse.getEpoch(), sampleTailsResponse.getLogTail(),
                        sampleTailsResponse.getStreamTails())
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), sampleTailsResponse);
    }

    /**
     * Test that the LogUnitHandler correctly handles a COMMITTED_TAIL_RESPONSE.
     */
    @Test
    public void testGetCommittedTail() {
        long sampleCommittedTail = 5L;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getCommittedTailResponseMsg(sampleCommittedTail)
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), sampleCommittedTail);
    }

    /**
     * Test that the LogUnitHandler correctly handles a UPDATE_COMMITTED_TAIL_RESPONSE.
     */
    @Test
    public void testUpdateCommittedTail() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getUpdateCommittedTailResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a LOG_ADDRESS_SPACE_RESPONSE.
     */
    @Test
    public void testGetLogAddressSpace() {
        StreamsAddressResponse addressResponse = new StreamsAddressResponse(0L, new HashMap<>());
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getLogAddressSpaceResponseMsg(addressResponse.getLogTail(), addressResponse.getEpoch(),
                        addressResponse.getAddressMap())
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), addressResponse);
    }

    /**
     * Test that the LogUnitHandler correctly handles a TRIM_MARK_RESPONSE.
     */
    @Test
    public void testGetTrimMark() {
        long sampleTrimMark = 5L;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getTrimMarkResponseMsg(sampleTrimMark)
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), sampleTrimMark);
    }

    /**
     * Test that the LogUnitHandler correctly handles a KNOWN_ADDRESS_RESPONSE.
     */
    @Test
    public void testRequestKnownAddresses() {
        KnownAddressResponse knownAddressResponse = new KnownAddressResponse(new HashSet<>());
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getKnownAddressResponseMsg(knownAddressResponse.getKnownAddresses())
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), knownAddressResponse);
    }

    /**
     * Test that the LogUnitHandler correctly handles a TRIM_LOG_RESPONSE.
     */
    @Test
    public void testPrefixTrim() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getTrimLogResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a COMPACT_RESPONSE.
     */
    @Test
    public void testCompact() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getCompactResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a FLUSH_CACHE_RESPONSE.
     */
    @Test
    public void testFlushCache() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getFlushCacheResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly handles a RESET_LOG_UNIT_RESPONSE.
     */
    @Test
    public void testResetLogUnit() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, false),
                getResetLogUnitResponseMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the LogUnitHandler correctly throws TrimmedException upon TRIMMED_ERROR.
     */
    @Test
    public void testTrimmedError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, true),
                getTrimmedErrorMsg()
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed exceptionally.
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), any(TrimmedException.class));
    }

    /**
     * Test that the LogUnitHandler correctly throws OverwriteException upon OVERWRITE_ERROR.
     */
    @Test
    public void testOverwriteError() {
        int causeIdWrittenByHole = OverwriteCause.SAME_DATA.getId();
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, true),
                getOverwriteErrorMsg(causeIdWrittenByHole)
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<OverwriteException> captor = ArgumentCaptor.forClass(OverwriteException.class);
        // Verify that the correct request was completed exceptionally.
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), captor.capture());
        assertEquals(causeIdWrittenByHole, captor.getValue().getOverWriteCause().getId());
    }

    /**
     * Test that the LogUnitHandler correctly throws DataCorruptionException upon DATA_CORRUPTION_ERROR.
     */
    @Test
    public void testDataCorruptionError() {
        long sampleAddress = 5L;
        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, true),
                getDataCorruptionErrorMsg(sampleAddress)
        );

        logUnitHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed exceptionally.
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), any(DataCorruptionException.class));
    }
}
