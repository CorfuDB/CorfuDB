package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TailRequestMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamsAddressResponse;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCommittedTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCompactRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getFlushCacheRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getInspectAddressesRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getKnownAddressRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getLogAddressSpaceRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getRangeWriteLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getResetLogUnitRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailsResponse;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimMarkRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getUpdateCommittedTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Slf4j
public class LogUnitServerTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The LogUnitServer instance used for testing.
    private LogUnitServer logUnitServer;

    // Objects that need to be mocked.
    private ServerContext mServerContext;
    private IServerRouter mServerRouter;
    private ChannelHandlerContext mChannelHandlerContext;
    private BatchProcessor mBatchProcessor;
    private StreamLog mStreamLog;
    private LogUnitServerCache mCache;

    private final AtomicInteger requestCounter = new AtomicInteger();
    private final String PAYLOAD_DATA = "PAYLOAD";

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 1L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that compares the base fields of two message headers.
     * These include the request ID, the epoch, the client ID, and the cluster ID.
     *
     * @param requestHeader   the header from the request message
     * @param responseHeader  the header from the response message
     * @return                true if the two headers have the same base field values
     */
    private boolean compareBaseHeaderFields(HeaderMsg requestHeader, HeaderMsg responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    /**
     * A helper method that creates a simple LogData object with default values.
     *
     * @param address LogData's global address
     * @return        the corresponding LogData
     */
    private LogData getDefaultLogData(long address) {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(PAYLOAD_DATA.getBytes(), b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(address);
        ld.setEpoch(1L);
        return ld;
    }

    /**
     * Perform the required preparation before running individual tests.
     * This includes preparing the mocks and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {
        mServerContext = mock(ServerContext.class);
        mServerRouter = mock(IServerRouter.class);
        mChannelHandlerContext = mock(ChannelHandlerContext.class);
        mBatchProcessor = mock(BatchProcessor.class);
        mStreamLog = mock(StreamLog.class);
        mCache = mock(LogUnitServerCache.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread.
        when(mServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        // Initialize basic LogUnit server parameters.
        when(mServerContext.getServerConfig())
                .thenReturn(ImmutableMap.of(
                        "--cache-heap-ratio", "0.5",
                        "--memory", false,
                        "--no-verify", false,
                        "--no-sync", false));

        // Prepare the LogUnitServerInitializer.
        LogUnitServer.LogUnitServerInitializer mLUSI = mock(LogUnitServer.LogUnitServerInitializer.class);
        when(mLUSI.buildStreamLog(any(LogUnitServerConfig.class), eq(mServerContext))).thenReturn(mStreamLog);
        when(mLUSI.buildLogUnitServerCache(any(LogUnitServerConfig.class), eq(mStreamLog))).thenReturn(mCache);
        when(mLUSI.buildStreamLogCompaction(mStreamLog)).thenReturn(mock(StreamLogCompaction.class));
        when(mLUSI.buildBatchProcessor(any(LogUnitServerConfig.class),
                eq(mStreamLog), eq(mServerContext))).thenReturn(mBatchProcessor);

        logUnitServer = new LogUnitServer(mServerContext, mLUSI);
    }

    /**
     * Test that the LogUnitServer correctly handles a TAIL request. A
     * TAILS_QUERY operation should be added to the BatchProcessor, and
     * the response should contain the result from the completed future.
     */
    @Test
    public void testHandleTail() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getTailRequestMsg(TailRequestMsg.Type.ALL_STREAMS_TAIL)
        );

        TailsResponse tailsResponseExpected = new TailsResponse(1L, 20L,
                ImmutableMap.of(UUID.randomUUID(), 5L, UUID.randomUUID(), 10L));

        when(mBatchProcessor.<TailsResponse>addTask(BatchWriterOperation.Type.TAILS_QUERY, request))
                .thenReturn(CompletableFuture.completedFuture(tailsResponseExpected));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a TAIL response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTailResponse());

        // Assert that the response is as expected.
        TailsResponse provided = getTailsResponse(response.getPayload().getTailResponse());
        assertEquals(tailsResponseExpected.getEpoch(), provided.getEpoch());
        assertEquals(tailsResponseExpected.getLogTail(), provided.getLogTail());
        assertEquals(tailsResponseExpected.getStreamTails(), provided.getStreamTails());
    }

    /**
     * Test that the LogUnitServer correctly handles a LOG_ADDRESS_SPACE request.
     * A LOG_ADDRESS_SPACE_QUERY operation should be added to the BatchProcessor,
     * and the response should contain the result from the completed future.
     */
    @Test
    public void testHandleLogAddressSpace() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getLogAddressSpaceRequestMsg()
        );

        Map<UUID, StreamAddressSpace> tails = ImmutableMap.of(
                UUID.randomUUID(), new StreamAddressSpace(-1L, Roaring64NavigableMap.bitmapOf(32L)),
                UUID.randomUUID(), new StreamAddressSpace(-1L, Roaring64NavigableMap.bitmapOf(11L))
        );

        StreamsAddressResponse expectedResponse = new StreamsAddressResponse(32L, tails);
        expectedResponse.setEpoch(1L);

        // Return a future when the operation is added to the BatchProcessor.
        when(mBatchProcessor.<StreamsAddressResponse>addTask(BatchWriterOperation.Type.LOG_ADDRESS_SPACE_QUERY, request))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a LOG_ADDRESS_SPACE response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasLogAddressSpaceResponse());

        // Assert that the response is as expected.
        StreamsAddressResponse provided = getStreamsAddressResponse(
                response.getPayload().getLogAddressSpaceResponse().getLogTail(),
                response.getPayload().getLogAddressSpaceResponse().getEpoch(),
                response.getPayload().getLogAddressSpaceResponse().getAddressMapList()
        );

        assertEquals(expectedResponse.getEpoch(), provided.getEpoch());
        assertEquals(expectedResponse.getLogTail(), provided.getLogTail());
        assertEquals(expectedResponse.getAddressMap().size(), provided.getAddressMap().size());

        provided.getAddressMap().forEach((id, addressSpace) -> {
            assertEquals(expectedResponse.getAddressMap().get(id).getTrimMark(), addressSpace.getTrimMark());
            assertEquals(expectedResponse.getAddressMap().get(id).getAddressMap(), addressSpace.getAddressMap());
        });
    }

    /**
     * Test that the LogUnitServer correctly handles a TRIM_MARK request.
     */
    @Test
    public void testHandleTrimMark() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getTrimMarkRequestMsg()
        );

        final long trimMark = 15L;
        when(mStreamLog.getTrimMark()).thenReturn(trimMark);

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a TRIM_MARK response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTrimMarkResponse());
        assertEquals(trimMark, response.getPayload().getTrimMarkResponse().getTrimMark());
    }

    /**
     * Test that the LogUnitServer correctly handles a COMMITTED_TAIL request.
     */
    @Test
    public void testHandleCommittedTail() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getCommittedTailRequestMsg()
        );

        final long tail = 7L;
        when(mStreamLog.getCommittedTail()).thenReturn(tail);

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a COMMITTED_TAIL response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasCommittedTailResponse());
        assertEquals(tail, response.getPayload().getCommittedTailResponse().getCommittedTail());
    }

    /**
     * Test that the LogUnitServer correctly handles an UPDATE_COMMITTED_TAIL
     * request.
     */
    @Test
    public void testHandleUpdateCommittedTail() {
        final long tail = 7L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getUpdateCommittedTailRequestMsg(tail)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has an UPDATE_COMMITTED_TAIL response and that
        // the base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasUpdateCommittedTailResponse());

        // Verify that the tail has been updated in the StreamLog.
        verify(mStreamLog).updateCommittedTail(tail);
    }

    /**
     * Test that the LogUnitServer correctly handles a TRIM_LOG request.
     * A PREFIX_TRIM operation should be added to the BatchProcessor and
     * a TRIM_LOG response should be sent once the future is completed.
     */
    @Test
    public void testHandleTrimLog() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getTrimLogRequestMsg(Token.of(0L, 24L))
        );

        // Return a future when the operation is added to the BatchProcessor.
        when(mBatchProcessor.<Void>addTask(BatchWriterOperation.Type.PREFIX_TRIM, request))
                .thenReturn(CompletableFuture.completedFuture(null));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a TRIM_LOG response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTrimLogResponse());
    }

    /**
     * Test that the LogUnitServer correctly handles an INSPECT_ADDRESSES request.
     * We inspect the addresses [1, 16], where the addresses [10, 16] are empty.
     * This should be reflected in the INSPECT_ADDRESSES response.
     */
    @Test
    public void testHandleInspectAddresses() {
        final List<Long> addresses = LongStream.rangeClosed(1L, 16L).boxed().collect(Collectors.toList());
        final List<Long> expectedEmpty = addresses.stream()
                .filter(address -> address > 9L).collect(Collectors.toList());

        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getInspectAddressesRequestMsg(addresses)
        );

        addresses.forEach(address -> when(mStreamLog.contains(address)).thenReturn(address < 10L));
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has an INSPECT_ADDRESSES response and that the
        // base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasInspectAddressesResponse());

        // Assert that the response contains the expected addresses.
        assertEquals(expectedEmpty, response.getPayload().getInspectAddressesResponse().getEmptyAddressList());
    }

    /**
     * Test that the LogUnitServer correctly handles an INSPECT_ADDRESSES request
     * when an address is trimmed. We inspect the addresses [1, 16], but inspecting
     * address 11 causes trimmed exception.
     */
    @Test
    public void testHandleInspectAddressesTrimmed() {
        final List<Long> addresses = LongStream.rangeClosed(1L, 16L).boxed().collect(Collectors.toList());
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getInspectAddressesRequestMsg(addresses)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mStreamLog.contains(11L)).thenThrow(new TrimmedException());
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a TRIMMED error and that the base header
        // fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasTrimmedError());
    }

    /**
     * Test that the LogUnitServer correctly handles an INSPECT_ADDRESSES request
     * when an address is corrupted. We inspect the addresses [1, 16], but address
     * 11 is corrupted. This should be reflected in the DATA_CORRUPTION response.
     */
    @Test
    public void testHandleInspectAddressesCorrupted() {
        final List<Long> addresses = LongStream.rangeClosed(1L, 16L).boxed().collect(Collectors.toList());
        final long badAddress = 11L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getInspectAddressesRequestMsg(addresses)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mStreamLog.contains(badAddress)).thenThrow(new DataCorruptionException());
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a DATA_CORRUPTION error containing the bad
        // address and that the base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasDataCorruptionError());
        assertEquals(badAddress, response.getPayload().getServerError().getDataCorruptionError().getAddress());
    }

    /**
     * Test that the LogUnitServer correctly handles a KNOWN_ADDRESS request.
     */
    @Test
    public void testHandleKnownAddress() {
        final long startRange = 3L;
        final long endRange = 7L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getKnownAddressRequestMsg(startRange, endRange)
        );

        final Set<Long> rKnown = LongStream.rangeClosed(startRange, endRange).boxed().collect(Collectors.toSet());
        when(mStreamLog.getKnownAddressesInRange(startRange, endRange)).thenReturn(rKnown);
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a KNOWN_ADDRESS response and that the
        // base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasKnownAddressResponse());

        // Assert that the response contains the expected addresses.
        assertEquals(rKnown, new HashSet<>(response.getPayload().getKnownAddressResponse().getKnownAddressList()));
    }

    /**
     * Test that the LogUnitServer correctly handles a COMPACT request.
     */
    @Test
    public void testHandleCompact() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, true),
                getCompactRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a COMPACT response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasCompactResponse());

        // Verify that StreamLog was compacted.
        verify(mStreamLog).compact();
    }

    /**
     * Test that the LogUnitServer correctly handles a FLUSH_CACHE request.
     */
    @Test
    public void testHandleFlushCache() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, true),
                getFlushCacheRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a FLUSH_CACHE response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasFlushCacheResponse());

        // Verify that cache was flushed.
        verify(mCache).invalidateAll();
    }

    /**
     * Test that the LogUnitServer correctly handles a RESET_LOG_UNIT
     * request when the request has an older epoch.
     */
    @Test
    public void handleResetLogUnitOldEpoch() {
        sendAndValidateResetLogUnit(0L, 1L, Layout.INVALID_EPOCH);

        // Verify that no operation was added to the BatchProcessor and that
        // no invalidating took place.
        verify(mBatchProcessor, never()).addTask(any(BatchWriterOperation.Type.class), any(RequestMsg.class));
        verify(mCache, never()).invalidateAll();
        verify(mStreamLog, never()).reset();
    }

    /**
     * Test that the LogUnitServer correctly handles a RESET_LOG_UNIT
     * request when the request has an epoch that is not greater than
     * the last reset epoch seen by the server.
     */
    @Test
    public void handleResetLogUnitWatermarkEpoch() {
        sendAndValidateResetLogUnit(0L, 0L, 0L);

        // Verify that no operation was added to the BatchProcessor and that
        // no invalidating took place.
        verify(mBatchProcessor, never()).addTask(any(BatchWriterOperation.Type.class), any(RequestMsg.class));
        verify(mCache, never()).invalidateAll();
        verify(mStreamLog, never()).reset();
    }

    /**
     * Test that the LogUnitServer correctly handles a RESET_LOG_UNIT request
     * with a valid epoch.
     */
    @Test
    public void handleResetLogUnit() {
        RequestMsg request = sendAndValidateResetLogUnit(1L, 1L, Layout.INVALID_EPOCH);

        // Verify that a RESET operation was added to the BatchProcessor and that
        // the cache was invalidated.
        verify(mBatchProcessor).addTask(BatchWriterOperation.Type.RESET, request);
        verify(mCache).invalidateAll();
    }

    /**
     * A helper method that sends and performs basic validation of
     * a RESET_LOG_UNIT request with the following test parameters.
     * The request is returned so that the caller can perform additional
     * validation if desired.
     *
     * @param requestEpoch    the request epoch
     * @param serverEpoch     the server epoch
     * @param watermarkEpoch  the watermark epoch
     * @return                the RESET_LOG_UNIT request processed by the server
     */
    private RequestMsg sendAndValidateResetLogUnit(long requestEpoch, long serverEpoch, long watermarkEpoch) {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, true),
                getResetLogUnitRequestMsg(requestEpoch)
        );

        when(mServerContext.getServerEpoch()).thenReturn(serverEpoch);
        when(mServerContext.getLogUnitEpochWaterMark()).thenReturn(watermarkEpoch);

        // Prepare a future that can be used by the caller.
        when(mBatchProcessor.<Void>addTask(BatchWriterOperation.Type.RESET, request))
                .thenReturn(CompletableFuture.completedFuture(null));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a RESET_LOG_UNIT response and that the
        // base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasResetLogUnitResponse());
        return request;
    }

    /**
     * Test that the LogUnitServer correctly handles a READ_LOG request.
     * We request to read the addresses [1, 10]. The addresses [1, 7]
     * contain some data but the addresses [8, 10] are empty. This should
     * be reflected in the READ_LOG response.
     */
    @Test
    public void testHandleReadLog() {
        final List<Long> addresses = LongStream.rangeClosed(1L, 10L).boxed().collect(Collectors.toList());
        final boolean cacheable = true;

        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getReadLogRequestMsg(addresses, cacheable)
        );

        addresses.forEach(address -> when(mCache.get(address, cacheable))
                .thenReturn(address < 8L ? getDefaultLogData(address) : null));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a READ_LOG response and that the
        // base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasReadLogResponse());

        // Verify that the addresses [1, 7] contain the expected data
        // and that the addresses [8, 10] are empty.
        ReadResponse rr = getReadResponse(response.getPayload().getReadLogResponse());
        rr.getAddresses().forEach((address, ld) -> {
            if (address < 8L) {
                assertEquals(DataType.DATA, ld.getType());
                assertArrayEquals(PAYLOAD_DATA.getBytes(), (byte[]) ld.getPayload(null));
                assertEquals((Long) 1L, ld.getEpoch());
            } else {
                assertEquals(DataType.EMPTY, ld.getType());
                assertNull(ld.getData());
            }

            assertEquals(address, ld.getGlobalAddress());
        });
    }

    /**
     * Test that the LogUnitServer correctly handles a READ_LOG request
     * when a provided address contains corrupt data. We request to read
     * the addresses [1, 10], but address 7 is corrupted. This should be
     * reflected in the server error response.
     */
    @Test
    public void testHandleReadLogCorrupted() {
        final List<Long> addresses = LongStream.rangeClosed(1L, 10L).boxed().collect(Collectors.toList());
        final boolean cacheable = false;

        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getReadLogRequestMsg(addresses, cacheable)
        );

        addresses.forEach(address -> {
            if (address != 7L) {
                when(mCache.get(address, cacheable)).thenReturn(getDefaultLogData(address));
            } else {
                when(mCache.get(address, cacheable)).thenThrow(new DataCorruptionException());
            }
        });

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a DATA_CORRUPTION error response with the
        // expected address, and that the base header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasDataCorruptionError());
        assertEquals(7L, response.getPayload().getServerError().getDataCorruptionError().getAddress());
    }

    /**
     * Test that the LogUnitServer correctly handles a WRITE_LOG request.
     * A WRITE operation should be added to the BatchProcessor for this
     * request.
     */
    @Test
    public void testHandleWriteLog() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getWriteLogRequestMsg(getDefaultLogData(1L))
        );

        // Prepare a future that can be used by the caller.
        when(mBatchProcessor.<Void>addTask(BatchWriterOperation.Type.WRITE, request))
                .thenReturn(CompletableFuture.completedFuture(null));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasWriteLogResponse());
    }

    /**
     * Test that the LogUnitServer correctly handles a RANGE_WRITE_LOG
     * request. A RANGE_WRITE operation should be added to the BatchProcessor
     * for this request.
     */
    @Test
    public void testHandleRangeWriteLog() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(false, false),
                getRangeWriteLogRequestMsg(Collections.singletonList(getDefaultLogData(1L)))
        );

        // Prepare a future that can be used by the caller.
        when(mBatchProcessor.<Void>addTask(BatchWriterOperation.Type.RANGE_WRITE, request))
                .thenReturn(CompletableFuture.completedFuture(null));

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasRangeWriteLogResponse());
    }
}
