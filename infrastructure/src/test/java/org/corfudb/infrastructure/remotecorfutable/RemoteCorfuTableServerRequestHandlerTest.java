package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.common.remotecorfutable.DatabaseConstants.DATABASE_CHARSET;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerCache;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import org.corfudb.protocols.CorfuProtocolRemoteCorfuTable;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getContainsKeyRequestMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getContainsValueRequestMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getGetRequestMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getScanRequestMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getSizeRequestMsg;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.RemoteCorfuTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class holds unit tests for the RemoteCorfuTableRequestHandler on the Log Unit Server.
 *
 * Created by nvaishampayan517 on 08/10/21
 */
@Slf4j
public class RemoteCorfuTableServerRequestHandlerTest {
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
    private DatabaseHandler mDatabaseHandler;
    private RemoteCorfuTableRequestHandler mRemoteCorfuTableRequestHandler;

    private Map<ByteString, ByteString> database;
    private AtomicInteger requestCounter;
    private final UUID stream1 = UUID.nameUUIDFromBytes("stream1".getBytes(DATABASE_CHARSET));

    /**
     * A helper method that creates a basic message header populated
     * with default values. Duplicate from LogUnitServerTest.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private CorfuMessage.HeaderMsg getBasicHeader(CorfuProtocolMessage.ClusterIdCheck ignoreClusterId, CorfuProtocolMessage.EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), CorfuMessage.PriorityLevel.NORMAL, 1L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that compares the base fields of two message headers.
     * These include the request ID, the epoch, the client ID, and the cluster ID.
     * Duplicate from LogUnitServerTest.
     *
     * @param requestHeader   the header from the request message
     * @param responseHeader  the header from the response message
     * @return                true if the two headers have the same base field values
     */
    private boolean compareBaseHeaderFields(CorfuMessage.HeaderMsg requestHeader, CorfuMessage.HeaderMsg responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    @Before
    public void setup() throws RocksDBException {
        mServerContext = mock(ServerContext.class);
        mServerRouter = mock(IServerRouter.class);
        mChannelHandlerContext = mock(ChannelHandlerContext.class);
        mBatchProcessor = mock(BatchProcessor.class);
        mStreamLog = mock(StreamLog.class);
        mCache = mock(LogUnitServerCache.class);
        mDatabaseHandler = mock(DatabaseHandler.class);


        // Initialize basic LogUnit server parameters.
        when(mServerContext.getServerConfig())
                .thenReturn(ImmutableMap.of(
                        "--cache-heap-ratio", "0.5",
                        "--memory", false,
                        "--no-verify", false,
                        "--no-sync", false));

        when(mServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        requestCounter = new AtomicInteger();
        //mock database functions
        database = new LinkedHashMap<>();

        doAnswer(invocationOnMock -> {
            RemoteCorfuTableVersionedKey key = invocationOnMock.getArgument(0);
            return CompletableFuture.completedFuture(database.get(key.getEncodedVersionedKey()));
        }).when(mDatabaseHandler).getAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(5, invocationOnMock.getArgument(0),
                invocationOnMock.getArgument(1))).when(mDatabaseHandler).scanAsync(any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(
                new RemoteCorfuTableVersionedKey(database.keySet().iterator().next()),
                invocationOnMock.getArgument(0), invocationOnMock.getArgument(1),
                invocationOnMock.getArgument(2)))
                .when(mDatabaseHandler).scanAsync(anyInt(),any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(invocationOnMock.getArgument(0),5,
                invocationOnMock.getArgument(1), invocationOnMock.getArgument(2))).when(mDatabaseHandler)
                .scanAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> {
            boolean first = true;
            boolean removeFirst = true;
            boolean started = false;
            RemoteCorfuTableVersionedKey startKey = invocationOnMock.getArgument(0);
            int numEntries = invocationOnMock.getArgument(1);
            List<RemoteCorfuTableEntry> results = new LinkedList<>();
            for (Map.Entry<ByteString, ByteString> entry: database.entrySet()) {
                if (results.size() > numEntries) {
                    break;
                }
                if (first) {
                    if (entry.getKey().equals(startKey.getEncodedVersionedKey())) {
                        removeFirst = false;
                        started = true;
                        results.add(new RemoteCorfuTableEntry(
                                new RemoteCorfuTableVersionedKey(entry.getKey()), entry.getValue()));
                    }
                    first = false;
                } else {
                    if (!started) {
                        if (entry.getKey().equals(startKey.getEncodedVersionedKey())) {
                            started = true;
                            results.add(new RemoteCorfuTableEntry(
                                    new RemoteCorfuTableVersionedKey(entry.getKey()), entry.getValue()));
                        }
                    } else {
                        results.add(new RemoteCorfuTableEntry(
                                new RemoteCorfuTableVersionedKey(entry.getKey()), entry.getValue()));
                    }
                }
            }
            if (removeFirst) {
                results.remove(0);
            } else if (results.size() > numEntries) {
                results.remove(results.size()-1);
            }
            return CompletableFuture.completedFuture(results);
        }).when(mDatabaseHandler).scanAsync(any(RemoteCorfuTableVersionedKey.class), anyInt(), any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> {
            ByteString mapKey =
                    invocationOnMock.getArgument(0, RemoteCorfuTableVersionedKey.class).getEncodedVersionedKey();
            return CompletableFuture.completedFuture(database.containsKey(mapKey));
        }).when(mDatabaseHandler).containsKeyAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

        doAnswer(invocationOnMock -> {
            ByteString mapVal =
                    invocationOnMock.getArgument(0);
            return CompletableFuture.completedFuture(database.containsValue(mapVal));
        }).when(mDatabaseHandler).containsValueAsync(any(ByteString.class), any(UUID.class), anyLong(), anyInt());

        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(database.size())).when(mDatabaseHandler)
                .sizeAsync(any(UUID.class), anyLong(), anyInt());

        // Prepare the LogUnitServerInitializer.
        LogUnitServer.LogUnitServerInitializer mLUSI = mock(LogUnitServer.LogUnitServerInitializer.class);
        when(mLUSI.buildStreamLog(any(LogUnitServer.LogUnitServerConfig.class), eq(mServerContext))).thenReturn(mStreamLog);
        when(mLUSI.buildLogUnitServerCache(any(LogUnitServer.LogUnitServerConfig.class), eq(mStreamLog))).thenReturn(mCache);
        when(mLUSI.buildStreamLogCompaction(mStreamLog)).thenReturn(mock(StreamLogCompaction.class));
        when(mLUSI.buildBatchProcessor(any(LogUnitServer.LogUnitServerConfig.class),
                eq(mStreamLog), eq(mServerContext))).thenReturn(mBatchProcessor);
        when(mLUSI.buildDatabaseHandler(any(Path.class), any(Options.class),
                any(ExecutorService.class))).thenReturn(mDatabaseHandler);

        mRemoteCorfuTableRequestHandler = new RemoteCorfuTableRequestHandler(mDatabaseHandler);
        when(mLUSI.buildRemoteCorfuTableRequestHandler(any(DatabaseHandler.class)))
                .thenReturn(mRemoteCorfuTableRequestHandler);

        logUnitServer = new LogUnitServer(mServerContext, mLUSI);
    }

    @Test
    public void testGetRequestHandling() {
        //place dummy value in map to read
        RemoteCorfuTableVersionedKey dummyKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), 1L);
        ByteString dummyValue = ByteString.copyFrom("dummyValue", DATABASE_CHARSET);
        database.put(dummyKey.getEncodedVersionedKey(), dummyValue);

        CorfuMessage.RequestMsg request = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getGetRequestMsg(stream1, dummyKey)
        );

        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        //capture response
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        CorfuMessage.ResponseMsg responseMsg = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), responseMsg.getHeader()));
        assertTrue(responseMsg.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(responseMsg.getPayload().getRemoteCorfuTableResponse().hasGetResponse());

        //verify response contents
        RemoteCorfuTable.RemoteCorfuTableGetResponseMsg getResponseMsg =
                responseMsg.getPayload().getRemoteCorfuTableResponse().getGetResponse();
        RemoteCorfuTable.RemoteCorfuTableGetResponseMsg expectedResponse =
                RemoteCorfuTable.RemoteCorfuTableGetResponseMsg.newBuilder().setPayloadValue(dummyValue).build();
        assertEquals(getResponseMsg.getPayloadValue(), expectedResponse.getPayloadValue());
    }

    @Test
    public void testDefaultSizeScans() {
        for (int i = 0; i < 10; i++) {
            RemoteCorfuTableVersionedKey dummyKey =
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), i);
            ByteString dummyValue = ByteString.copyFrom("dummyValue" + i, DATABASE_CHARSET);
            database.put(dummyKey.getEncodedVersionedKey(), dummyValue);
        }
        CorfuMessage.RequestMsg request1 = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getScanRequestMsg(0, stream1, 10)
        );
        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(request1, mChannelHandlerContext, mServerRouter);

        List<RemoteCorfuTableEntry> entries = database.entrySet().stream().map(
                entry -> new RemoteCorfuTableEntry(
                        new RemoteCorfuTableVersionedKey(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        RemoteCorfuTableVersionedKey cursor = entries.get(4).getKey();
        CorfuMessage.RequestMsg request2 = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getScanRequestMsg(cursor,0, stream1, 10)
        );
        logUnitServer.handleMessage(request2, mChannelHandlerContext, mServerRouter);

        //capture responses
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        CorfuMessage.ResponseMsg startingResponse = responseCaptor.getAllValues().get(0);
        assertTrue(compareBaseHeaderFields(request1.getHeader(), startingResponse.getHeader()));
        assertTrue(startingResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(startingResponse.getPayload().getRemoteCorfuTableResponse().hasScanResponse());

        RemoteCorfuTable.RemoteCorfuTableScanResponseMsg startingScanresponse = startingResponse.getPayload()
                .getRemoteCorfuTableResponse().getScanResponse();
        List<RemoteCorfuTableEntry> startingEntries = startingScanresponse.getEntriesList()
                .stream().map(CorfuProtocolRemoteCorfuTable::getEntryFromMsg).collect(Collectors.toList());
        assertEquals(5, startingEntries.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(entries.get(i), startingEntries.get(i));
        }

        CorfuMessage.ResponseMsg cursorResponse = responseCaptor.getAllValues().get(1);
        assertTrue(compareBaseHeaderFields(request2.getHeader(), cursorResponse.getHeader()));
        assertTrue(cursorResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(cursorResponse.getPayload().getRemoteCorfuTableResponse().hasScanResponse());
        RemoteCorfuTable.RemoteCorfuTableScanResponseMsg cursorScanResponse = cursorResponse.getPayload()
                .getRemoteCorfuTableResponse().getScanResponse();
        List<RemoteCorfuTableEntry> cursorEntries = cursorScanResponse.getEntriesList()
                .stream().map(CorfuProtocolRemoteCorfuTable::getEntryFromMsg).collect(Collectors.toList());
        assertEquals(5, cursorEntries.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(entries.get(i+5), cursorEntries.get(i));
        }
    }

    @Test
    public void testVarSizeScans() {
        for (int i = 0; i < 10; i++) {
            RemoteCorfuTableVersionedKey dummyKey =
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), i);
            ByteString dummyValue = ByteString.copyFrom("dummyValue" + i, DATABASE_CHARSET);
            database.put(dummyKey.getEncodedVersionedKey(), dummyValue);
        }
        CorfuMessage.RequestMsg request1 = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getScanRequestMsg(7, stream1, 10)
        );
        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(request1, mChannelHandlerContext, mServerRouter);

        List<RemoteCorfuTableEntry> entries = database.entrySet().stream().map(
                entry -> new RemoteCorfuTableEntry(
                        new RemoteCorfuTableVersionedKey(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        RemoteCorfuTableVersionedKey cursor = entries.get(6).getKey();
        CorfuMessage.RequestMsg request2 = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getScanRequestMsg(cursor,3, stream1, 10)
        );
        logUnitServer.handleMessage(request2, mChannelHandlerContext, mServerRouter);

        //capture responses
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        CorfuMessage.ResponseMsg startingResponse = responseCaptor.getAllValues().get(0);
        assertTrue(compareBaseHeaderFields(request1.getHeader(), startingResponse.getHeader()));
        assertTrue(startingResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(startingResponse.getPayload().getRemoteCorfuTableResponse().hasScanResponse());

        RemoteCorfuTable.RemoteCorfuTableScanResponseMsg startingScanresponse = startingResponse.getPayload()
                .getRemoteCorfuTableResponse().getScanResponse();
        List<RemoteCorfuTableEntry> startingEntries = startingScanresponse.getEntriesList()
                .stream().map(CorfuProtocolRemoteCorfuTable::getEntryFromMsg).collect(Collectors.toList());
        assertEquals(7, startingEntries.size());
        for (int i = 0; i < 7; i++) {
            assertEquals(entries.get(i), startingEntries.get(i));
        }

        CorfuMessage.ResponseMsg cursorResponse = responseCaptor.getAllValues().get(1);
        assertTrue(compareBaseHeaderFields(request2.getHeader(), cursorResponse.getHeader()));
        assertTrue(cursorResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(cursorResponse.getPayload().getRemoteCorfuTableResponse().hasScanResponse());
        RemoteCorfuTable.RemoteCorfuTableScanResponseMsg cursorScanResponse = cursorResponse.getPayload()
                .getRemoteCorfuTableResponse().getScanResponse();
        List<RemoteCorfuTableEntry> cursorEntries = cursorScanResponse.getEntriesList()
                .stream().map(CorfuProtocolRemoteCorfuTable::getEntryFromMsg).collect(Collectors.toList());
        assertEquals(3, cursorEntries.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(entries.get(i+7), cursorEntries.get(i));
        }
    }

    @Test
    public void testContainsKeyHandling() {
        //place dummy value in map to read
        RemoteCorfuTableVersionedKey dummyKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), 1L);
        ByteString dummyValue = ByteString.copyFrom("dummyValue", DATABASE_CHARSET);
        database.put(dummyKey.getEncodedVersionedKey(), dummyValue);
        CorfuMessage.RequestMsg trueRequest = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getContainsKeyRequestMsg(stream1, dummyKey)
        );
        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(trueRequest, mChannelHandlerContext, mServerRouter);
        CorfuMessage.RequestMsg falseRequest = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getContainsKeyRequestMsg(stream1, new RemoteCorfuTableVersionedKey("notPresent".getBytes(StandardCharsets.UTF_8)))
        );
        logUnitServer.handleMessage(falseRequest, mChannelHandlerContext, mServerRouter);
        //capture responses
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));

        CorfuMessage.ResponseMsg trueResponse = responseCaptor.getAllValues().get(0);
        assertTrue(compareBaseHeaderFields(trueRequest.getHeader(), trueResponse.getHeader()));
        assertTrue(trueResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(trueResponse.getPayload().getRemoteCorfuTableResponse().hasContainsResponse());
        assertTrue(trueResponse.getPayload().getRemoteCorfuTableResponse().getContainsResponse().getContains());

        CorfuMessage.ResponseMsg falseResponse = responseCaptor.getAllValues().get(1);
        assertTrue(compareBaseHeaderFields(falseRequest.getHeader(), falseResponse.getHeader()));
        assertTrue(falseResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(falseResponse.getPayload().getRemoteCorfuTableResponse().hasContainsResponse());
        assertFalse(falseResponse.getPayload().getRemoteCorfuTableResponse().getContainsResponse().getContains());
    }

    @Test
    public void testContainsValueHandling() {
        //place dummy value in map to read
        RemoteCorfuTableVersionedKey dummyKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), 1L);
        ByteString dummyValue = ByteString.copyFrom("dummyValue", DATABASE_CHARSET);
        database.put(dummyKey.getEncodedVersionedKey(), dummyValue);
        CorfuMessage.RequestMsg trueRequest = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getContainsValueRequestMsg(dummyValue, stream1, 2L, 1000)
        );
        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(trueRequest, mChannelHandlerContext, mServerRouter);
        CorfuMessage.RequestMsg falseRequest = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getContainsValueRequestMsg(ByteString.copyFrom("notPresent", StandardCharsets.UTF_8), stream1, 2L, 1000)
        );
        logUnitServer.handleMessage(falseRequest, mChannelHandlerContext, mServerRouter);
        //capture responses
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));

        CorfuMessage.ResponseMsg trueResponse = responseCaptor.getAllValues().get(0);
        assertTrue(compareBaseHeaderFields(trueRequest.getHeader(), trueResponse.getHeader()));
        assertTrue(trueResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(trueResponse.getPayload().getRemoteCorfuTableResponse().hasContainsResponse());
        assertTrue(trueResponse.getPayload().getRemoteCorfuTableResponse().getContainsResponse().getContains());

        CorfuMessage.ResponseMsg falseResponse = responseCaptor.getAllValues().get(1);
        assertTrue(compareBaseHeaderFields(falseRequest.getHeader(), falseResponse.getHeader()));
        assertTrue(falseResponse.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(falseResponse.getPayload().getRemoteCorfuTableResponse().hasContainsResponse());
        assertFalse(falseResponse.getPayload().getRemoteCorfuTableResponse().getContainsResponse().getContains());
    }

    @Test
    public void testSizeHandling() {
        for (int i = 0; i < 10; i++) {
            RemoteCorfuTableVersionedKey dummyKey =
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), i);
            ByteString dummyValue = ByteString.copyFrom("dummyValue" + i, DATABASE_CHARSET);
            database.put(dummyKey.getEncodedVersionedKey(), dummyValue);
        }

        CorfuMessage.RequestMsg request = getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                getSizeRequestMsg(stream1, 2L, 1000)
        );

        ArgumentCaptor<CorfuMessage.ResponseMsg> responseCaptor = ArgumentCaptor.forClass(CorfuMessage.ResponseMsg.class);
        logUnitServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        //capture response
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        CorfuMessage.ResponseMsg responseMsg = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), responseMsg.getHeader()));
        assertTrue(responseMsg.getPayload().hasRemoteCorfuTableResponse());
        assertTrue(responseMsg.getPayload().getRemoteCorfuTableResponse().hasSizeResponse());

        //verify response contents
        int size = responseMsg.getPayload().getRemoteCorfuTableResponse().getSizeResponse().getSize();
        assertEquals(10, size);
    }
}
