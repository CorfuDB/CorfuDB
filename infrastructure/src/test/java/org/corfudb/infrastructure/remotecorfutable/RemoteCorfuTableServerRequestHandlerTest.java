package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableMap;
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
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getGetRequestMsg;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.RemoteCorfuTable;
import static org.junit.Assert.assertEquals;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

    private Map<ByteString, ByteString> mDatabase;
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
                .thenReturn(Executors.newFixedThreadPool(4));

        requestCounter = new AtomicInteger();
        //mock database functions
        mDatabase = new ConcurrentHashMap<>();

        doAnswer(invocationOnMock -> {
            RemoteCorfuTableVersionedKey key = invocationOnMock.getArgument(0);
            return CompletableFuture.completedFuture(mDatabase.get(key.getEncodedVersionedKey()));
        }).when(mDatabaseHandler).getAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

        //TODO: temporary get mock
        doAnswer(invocationOnMock -> mDatabase.get(
                invocationOnMock.getArgument(0, RemoteCorfuTableVersionedKey.class).getEncodedVersionedKey()))
                .when(mDatabaseHandler).get(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(5, invocationOnMock.getArgument(0),
                invocationOnMock.getArgument(1))).when(mDatabaseHandler).scanAsync(any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(
                new RemoteCorfuTableVersionedKey(mDatabase.keySet().iterator().next().toByteArray()),
                invocationOnMock.getArgument(0), invocationOnMock.getArgument(1),
                invocationOnMock.getArgument(2)))
                .when(mDatabaseHandler).scanAsync(anyInt(),any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> mDatabaseHandler.scanAsync(invocationOnMock.getArgument(0),5,
                invocationOnMock.getArgument(1), invocationOnMock.getArgument(2))).when(mDatabaseHandler)
                .scanAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class), anyLong());

        doAnswer(invocationOnMock -> {
            boolean first = true;
            boolean removeFirst = false;
            boolean started = false;
            RemoteCorfuTableVersionedKey startKey = invocationOnMock.getArgument(0);
            int numEntries = invocationOnMock.getArgument(1);
            List<RemoteCorfuTableEntry> results = new LinkedList<>();
            for (Map.Entry<ByteString, ByteString> entry: mDatabase.entrySet()) {
                if (results.size() > numEntries) {
                    break;
                }
                if (first) {
                    if (entry.getKey().equals(startKey.getEncodedVersionedKey())) {
                        removeFirst = true;
                        started = true;
                        results.add(new RemoteCorfuTableEntry(
                                new RemoteCorfuTableVersionedKey(entry.getKey().toByteArray()), entry.getValue()));
                    }
                    first = false;
                } else {
                    if (!started) {
                        if (entry.getKey().equals(startKey.getEncodedVersionedKey())) {
                            started = true;
                            results.add(new RemoteCorfuTableEntry(
                                    new RemoteCorfuTableVersionedKey(entry.getKey().toByteArray()), entry.getValue()));
                        }
                    } else {
                        results.add(new RemoteCorfuTableEntry(
                                new RemoteCorfuTableVersionedKey(entry.getKey().toByteArray()), entry.getValue()));
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
            return CompletableFuture.completedFuture(mDatabase.containsKey(mapKey));
        }).when(mDatabaseHandler).containsKeyAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

        doAnswer(invocationOnMock -> {
            ByteString mapVal =
                    invocationOnMock.getArgument(0);
            return CompletableFuture.completedFuture(mDatabase.containsValue(mapVal));
        }).when(mDatabaseHandler).containsValueAsync(any(ByteString.class), any(UUID.class), anyLong(), anyInt());

        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(mDatabase.size())).when(mDatabaseHandler)
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
        //testing
        RemoteCorfuTable.RemoteCorfuTableResponseMsg.Builder corfuTableResponseBuilder = RemoteCorfuTable.RemoteCorfuTableResponseMsg.newBuilder();
        CorfuMessage.ResponsePayloadMsg.Builder payloadBuilder = CorfuMessage.ResponsePayloadMsg.newBuilder();
        //place dummy value in map to read
        RemoteCorfuTableVersionedKey dummyKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("dummyKey",DATABASE_CHARSET), 1L);
        ByteString dummyValue = ByteString.copyFrom("dummyValue", DATABASE_CHARSET);
        mDatabase.put(dummyKey.getEncodedVersionedKey(), dummyValue);

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
}
