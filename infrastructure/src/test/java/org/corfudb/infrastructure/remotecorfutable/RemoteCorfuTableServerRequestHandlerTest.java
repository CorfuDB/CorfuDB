package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerCache;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.junit.Before;
import org.junit.Rule;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

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

    private Map<ByteString, ByteString> mDatabase;

    @Before
    public void setup() {
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

        //mock database functions
        mDatabase = new ConcurrentHashMap<>();

        doAnswer(invocationOnMock -> {
            RemoteCorfuTableVersionedKey key = invocationOnMock.getArgument(0);
            return mDatabase.get(key.getEncodedVersionedKey());
        }).when(mDatabaseHandler).getAsync(any(RemoteCorfuTableVersionedKey.class), any(UUID.class));

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
            return results;
        }).when(mDatabaseHandler).scanAsync(any(RemoteCorfuTableVersionedKey.class), anyInt(), any(UUID.class), anyLong());

        // Prepare the LogUnitServerInitializer.
        LogUnitServer.LogUnitServerInitializer mLUSI = mock(LogUnitServer.LogUnitServerInitializer.class);
        when(mLUSI.buildStreamLog(any(LogUnitServer.LogUnitServerConfig.class), eq(mServerContext))).thenReturn(mStreamLog);
        when(mLUSI.buildLogUnitServerCache(any(LogUnitServer.LogUnitServerConfig.class), eq(mStreamLog))).thenReturn(mCache);
        when(mLUSI.buildStreamLogCompaction(mStreamLog)).thenReturn(mock(StreamLogCompaction.class));
        when(mLUSI.buildBatchProcessor(any(LogUnitServer.LogUnitServerConfig.class),
                eq(mStreamLog), eq(mServerContext))).thenReturn(mBatchProcessor);
        when(mLUSI.buildDatabaseHandler(any(Path.class), any(Options.class),
                any(ExecutorService.class))).thenReturn(mDatabaseHandler);

        logUnitServer = new LogUnitServer(mServerContext, mLUSI);
    }
}
