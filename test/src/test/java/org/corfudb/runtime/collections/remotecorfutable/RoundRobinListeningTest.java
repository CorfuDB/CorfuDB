package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import static org.assertj.core.api.Assertions.assertThat;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.RemoteCorfuTableRequestHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogListener;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RemoteCorfuTableListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RoundRobinListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutRequestMsg;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerRequestMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.LogUnitHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.SequencerHandler;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//TODO: mock tests
@Slf4j
public class RoundRobinListeningTest {
    private static EventLoopGroup NETTY_EVENT_LOOP;
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private CorfuRuntime runtime;
    private SingletonResource<CorfuRuntime> singletonRuntime;
    private RemoteCorfuTable<String, String> table;
    private RemoteCorfuTableListeningService logListener;
    private LogObserver observer;
    private final ISerializer serializer = Serializers.getDefaultSerializer();
    private CountDownLatch lock;

    private SequencerServer sequencerServer;
    private LayoutServer layoutServer;
    private LogUnitServer logUnitServer;
    private TestServerRouter serverRouter;

    private class LogObserver {
        @Getter
        private final List<SMROperation> receivedUpdates = new LinkedList<>();
        private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("TestObserver-%d").build());
        private final List<UUID> streamsToListen = new LinkedList<>();

        public void start() {
            exec.scheduleWithFixedDelay(this::pollTask, 0, 10, TimeUnit.MILLISECONDS);
        }

        public void addStream(UUID streamId) {
            streamsToListen.add(streamId);
        }

        private void pollTask() {
            for (UUID id: streamsToListen) {
                SMROperation op = logListener.getTask(id);
                if (op != null) {
                    receivedUpdates.add(op);
                    lock.countDown();
                    log.info("Current updates recieved: {}", receivedUpdates.size());
                }
            }
        }
    }

    @BeforeClass
    public static void initNetty() {
        NETTY_EVENT_LOOP =
                new DefaultEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                        new ThreadFactoryBuilder()
                                .setNameFormat("netty-%d")
                                .setDaemon(true)
                                .setUncaughtExceptionHandler((thread, throwable) -> {
                                    assertThat(false)
                                            .as("Thread " + thread.getName()
                                                    + " unexpectedly terminated with "
                                                    + throwable.getClass().getSimpleName())
                                            .isTrue();
                                })
                                .build());
    }

    @Before
    public void setupTable() {
        //same as Test Server Creation
        ServerContext serverContext = ServerContextBuilder.defaultTestContext(0);
        serverRouter = new TestServerRouter();
        sequencerServer = new SequencerServer(serverContext);
        layoutServer = new LayoutServer(serverContext);

        //spy on LUserver Initializer to disable log listener and DB
        LogUnitServer.LogUnitServerInitializer LUI = new LogUnitServer.LogUnitServerInitializer();
        LogUnitServer.LogUnitServerInitializer spyLUI = spy(LUI);
        DatabaseHandler mDbHandler = mock(DatabaseHandler.class);
        doReturn(mDbHandler).when(spyLUI).buildDatabaseHandler(
                any(Path.class), any(Options.class), any(ExecutorService.class));
        RemoteCorfuTableRequestHandler mReqHandler = mock(RemoteCorfuTableRequestHandler.class);
        doReturn(mReqHandler).when(spyLUI).buildRemoteCorfuTableRequestHandler(any(DatabaseHandler.class));
        RoundRobinListeningService mListeningService = mock(RoundRobinListeningService.class);
        doReturn(mListeningService).when(spyLUI).buildRemoteCorfuTableListeningService(
                any(ScheduledExecutorService.class), any(SingletonResource.class), anyLong());
        LogListener mListener = mock(LogListener.class);
        doReturn(mListener).when(spyLUI).buildLogListener(any(SingletonResource.class), any(DatabaseHandler.class),
                any(ScheduledExecutorService.class), anyLong(), any(RemoteCorfuTableListeningService.class));

        logUnitServer = new LogUnitServer(serverContext, spyLUI);

        serverRouter.addServer(sequencerServer);
        serverRouter.addServer(layoutServer);
        serverRouter.addServer(logUnitServer);
        serverContext.setServerRouter(serverRouter);
        serverRouter.setServerContext(serverContext);
        if(serverContext.isSingleNodeSetup() && serverContext.getCurrentLayout() != null){
            serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(),
                    this.serverRouter);
        }
        Layout layout = layoutServer.getCurrentLayout();
        layoutServer.handleMessage(getRequestMsg(
                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.IGNORE, CorfuProtocolMessage.EpochCheck.IGNORE),
                getBootstrapLayoutRequestMsg(layout)), null, serverRouter);

        sequencerServer
                .handleMessage(
                        CorfuProtocolMessage.getRequestMsg(
                                getBasicHeader(CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.CHECK),
                                getBootstrapSequencerRequestMsg(
                                        Collections.emptyMap(),
                                        0L,
                                        layout.getEpoch(),
                                        false)
                        ),
                        null, serverRouter);
        CorfuRuntime.overrideGetRouterFunction = this::getTestClientRouter;
        runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .nettyEventLoop(NETTY_EVENT_LOOP)
                .build());
        // Default number of times to read before hole filling to 0
        // (most aggressive, to surface concurrency issues).
        runtime.getParameters().setHoleFillRetry(0);
        runtime.addLayoutServer("test:0");


        singletonRuntime = SingletonResource.withInitial(this::getTestRuntime);
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(singletonRuntime.get(), "test1");
        logListener = new RoundRobinListeningService(
                Executors.newScheduledThreadPool(4,
                        new ThreadFactoryBuilder().setNameFormat("TestListener-%d").build()
                ), singletonRuntime, 10);
        observer = new LogObserver();
        observer.addStream(table.getStreamId());
        observer.start();
    }

    private CorfuRuntime getTestRuntime() {
        return runtime.connect();
    }

    //taken from AbstractViewTest
    private CorfuMessage.HeaderMsg getBasicHeader(CorfuProtocolMessage.ClusterIdCheck ignoreClusterId, CorfuProtocolMessage.EpochCheck ignoreEpoch) {
        return getHeaderMsg(1L, CorfuMessage.PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    private IClientRouter getTestClientRouter(CorfuRuntime runtime, String endpoint) {
        TestClientRouter tcn =
                new TestClientRouter(serverRouter);
        tcn.addClient(new BaseHandler())
                .addClient(new SequencerHandler())
                .addClient(new LayoutHandler())
                .addClient(new LogUnitHandler())
                .addClient(new ManagementHandler());
        return tcn;
    }

    @After
    public void shutdownTable() throws Exception {
        table.close();
        logListener.shutdown();
        singletonRuntime.cleanup(CorfuRuntime::shutdown);

        logUnitServer.shutdown();
        layoutServer.shutdown();
        sequencerServer.shutdown();

        // Abort any active transactions...
        while (runtime.getObjectsView().TXActive()) {
            runtime.getObjectsView().TXAbort();
        }

        runtime.shutdown();
    }

    @AfterClass
    public static void shutdownNetty() {
        NETTY_EVENT_LOOP.shutdownGracefully(100, 300, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    @Test
    public void listenBeforePut() throws InterruptedException {
        lock = new CountDownLatch(1);
        logListener.addStream(table.getStreamId());
        table.insert("Key", "Value");
        lock.await();
        List<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(1, updatesSeen.size());
        SMROperation insertOp = updatesSeen.get(0);
        assertEquals(RemoteCorfuTableSMRMethods.UPDATE, insertOp.getType());
        List<RemoteCorfuTableDatabaseEntry> dbEntries = insertOp.getEntryBatch();
        assertEquals(1, dbEntries.size());
        RemoteCorfuTableDatabaseEntry dbEntry = dbEntries.get(0);
        Object deserializedKey = deserializeObject(dbEntry.getKey().getEncodedKey());
        assertTrue(deserializedKey instanceof String);
        assertEquals("Key", deserializedKey);
        Object deserializedVal = deserializeObject(dbEntry.getValue());
        assertTrue(deserializedVal instanceof String);
        assertEquals("Value", deserializedVal);
    }



    @Test
    public void listenAfterPut() throws InterruptedException {
        lock = new CountDownLatch(1);
        table.insert("Key", "Value");
        logListener.addStream(table.getStreamId());
        lock.await();
        List<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(1, updatesSeen.size());
        SMROperation insertOp = updatesSeen.get(0);
        assertEquals(RemoteCorfuTableSMRMethods.UPDATE, insertOp.getType());
        List<RemoteCorfuTableDatabaseEntry> dbEntries = insertOp.getEntryBatch();
        assertEquals(1, dbEntries.size());
        RemoteCorfuTableDatabaseEntry dbEntry = dbEntries.get(0);
        Object deserializedKey = deserializeObject(dbEntry.getKey().getEncodedKey());
        assertTrue(deserializedKey instanceof String);
        assertEquals("Key", deserializedKey);
        Object deserializedVal = deserializeObject(dbEntry.getValue());
        assertTrue(deserializedVal instanceof String);
        assertEquals("Value", deserializedVal);
    }

    @Test
    public void testStrictOrderingOfSingleStreamUpdates() throws InterruptedException {
        lock = new CountDownLatch(1000);
        //if we perform an updateAll, all entries go into a single update
        //desired test is to test sequence of update entries, so we use multiple insert calls
        logListener.addStream(table.getStreamId());
        List<String> keys = new LinkedList<>();
        Map<String, String> pairs = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "Key" + i;
            String val = "Val" + i;
            keys.add(key);
            pairs.put(key, val);
            table.insert(key, val);
        }
        lock.await();
        List<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(keys.size(), updatesSeen.size());
        for (int i = 0; i < 1000; i++) {
            SMROperation insertOp = updatesSeen.get(i);
            assertEquals(RemoteCorfuTableSMRMethods.UPDATE, insertOp.getType());
            List<RemoteCorfuTableDatabaseEntry> dbEntries = insertOp.getEntryBatch();
            assertEquals(1, dbEntries.size());
            RemoteCorfuTableDatabaseEntry dbEntry = dbEntries.get(0);
            Object deserializedKey = deserializeObject(dbEntry.getKey().getEncodedKey());
            assertTrue(deserializedKey instanceof String);
            assertEquals(keys.get(i), deserializedKey);
            Object deserializedVal = deserializeObject(dbEntry.getValue());
            assertTrue(deserializedVal instanceof String);
            assertEquals(pairs.get(keys.get(i)), deserializedVal);
        }
    }

    @Test
    public void testOrderingOfStreamsInMultiStreamEnv() throws InterruptedException {
        lock = new CountDownLatch(1000);
        List<RemoteCorfuTable<String, String>> tables = new ArrayList<>(10);
        List<List<String>> tableKeys = new ArrayList<>(10);
        List<Map<String, String>> tablePairs = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            tables.add(RemoteCorfuTable.RemoteCorfuTableFactory.openTable(singletonRuntime.get(), "table" + i));
            logListener.addStream(tables.get(i).getStreamId());
            observer.addStream(tables.get(i).getStreamId());
            tableKeys.add(new ArrayList<>(100));
            tablePairs.add(new HashMap<>(100));
        }

        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "Table" + i + "Key" + j;
                String val = "Table" + i + "Val" + j;
                tableKeys.get(i).add(key);
                tablePairs.get(i).put(key, val);
                tables.get(i).insert(key, val);
            }
        }
        lock.await();
        List<SMROperation> updatesSeen = observer.getReceivedUpdates();
        Map<UUID, List<SMROperation>> groupedSMRops = updatesSeen.stream()
                .collect(Collectors.groupingBy(SMROperation::getStreamId));
        for (int i = 0; i < 10; i++) {
            List<SMROperation> tableOps = groupedSMRops.get(tables.get(i).getStreamId());
            assertNotNull(tableOps);
            assertEquals(100, tableOps.size());
            for (int j = 0; j < 100; j++) {
                SMROperation insertOp = tableOps.get(j);
                assertEquals(RemoteCorfuTableSMRMethods.UPDATE, insertOp.getType());
                List<RemoteCorfuTableDatabaseEntry> dbEntries = insertOp.getEntryBatch();
                assertEquals(1, dbEntries.size());
                RemoteCorfuTableDatabaseEntry dbEntry = dbEntries.get(0);
                Object deserializedKey = deserializeObject(dbEntry.getKey().getEncodedKey());
                assertTrue(deserializedKey instanceof String);
                assertEquals(tableKeys.get(i).get(j), deserializedKey);
                Object deserializedVal = deserializeObject(dbEntry.getValue());
                assertTrue(deserializedVal instanceof String);
                assertEquals(tablePairs.get(i).get(tableKeys.get(i).get(j)), deserializedVal);
            }
        }
    }

    @Test
    public void testRemoveStream() throws InterruptedException {
        logListener.addStream(table.getStreamId());
        for (int i = 0; i < 10000; i++) {
            table.insert("Key" + i, "Val" + i);
        }
        logListener.removeStream(table.getStreamId());
        Thread.sleep(10000);
        List<SMROperation> ops = observer.getReceivedUpdates();
        assertNotEquals(10000, ops.size());
    }

    //taken from RemoteCorfuTableAdapater
    private Object deserializeObject(ByteString serializedObject) {
        if (serializedObject.isEmpty()) {
            return null;
        }
        byte[] deserializationWrapped = new byte[serializedObject.size()];
        serializedObject.copyTo(deserializationWrapped, 0);
        ByteBuf deserializationBuffer = Unpooled.wrappedBuffer(deserializationWrapped);
        return serializer.deserialize(deserializationBuffer, singletonRuntime.get());
    }

}
