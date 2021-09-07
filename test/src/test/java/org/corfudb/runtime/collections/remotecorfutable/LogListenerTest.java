package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import static org.assertj.core.api.Assertions.assertThat;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.RemoteCorfuTableRequestHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogListener;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RemoteCorfuTableListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RoundRobinListeningService;
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
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LogListenerTest {
    private static EventLoopGroup NETTY_EVENT_LOOP;
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private SingletonResource<CorfuRuntime> singletonRuntime;
    private RemoteCorfuTable<String, String> table;
    private RemoteCorfuTableListeningService listeningService;
    private LogListener logListener;
    private final ISerializer serializer = Serializers.getDefaultSerializer();
    private CountDownLatch lock;

    //mock database to directly capture puts
    private ConcurrentMap<String, String> database;
    private DatabaseHandler mHandler;

    private SequencerServer sequencerServer;
    private LayoutServer layoutServer;
    private LogUnitServer logUnitServer;
    private TestServerRouter serverRouter;
    private CorfuRuntime runtime;

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
    public void setupTable() throws RocksDBException {
        //same as Test Server Creation
        ServerContext serverContext = ServerContextBuilder.defaultTestContext(0);
        serverRouter = new TestServerRouter();
        sequencerServer = new SequencerServer(serverContext);
        layoutServer = new LayoutServer(serverContext);

        //spy on LUserver Initializer to disable log listener and DB
        LogUnitServer.LogUnitServerInitializer LUI = new LogUnitServer.LogUnitServerInitializer();
        LogUnitServer.LogUnitServerInitializer spyLUI = spy(LUI);
        DatabaseHandler mDbHandler = mock(DatabaseHandler.class);
        doAnswer(invocationOnMock -> database.size()).when(mDbHandler).size(any(UUID.class), anyLong(), anyInt());
        doReturn(mDbHandler).when(spyLUI).buildDatabaseHandler(
                any(Path.class), any(Options.class), any(ExecutorService.class));
        //do not mock request handler so debugger can recieve SIZE responses
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
        listeningService = new RoundRobinListeningService(Executors.newScheduledThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("TestListenService-%d").build()),
                singletonRuntime, 10);
        mHandler = mock(DatabaseHandler.class);
        logListener = new LogListener(singletonRuntime, mHandler, Executors.newScheduledThreadPool(4,
                new ThreadFactoryBuilder().setNameFormat("TestListener-%d").build()),
                10, listeningService);
        database = new ConcurrentHashMap<>();

        doAnswer(invocationOnMock -> {
            String key = (String) deserializeObject(
                    invocationOnMock.getArgumentAt(0, RemoteCorfuTableVersionedKey.class).getEncodedKey());
            String val = (String) deserializeObject(
                    invocationOnMock.getArgumentAt(1, ByteString.class));
            database.put(key, val);
            lock.countDown();
            return null;
        }).when(mHandler).update(any(RemoteCorfuTableVersionedKey.class), any(ByteString.class), any(UUID.class));

        doAnswer(invocationOnMock -> {
            List<RemoteCorfuTableDatabaseEntry> entries =
                    (List<RemoteCorfuTableDatabaseEntry>) invocationOnMock.getArgumentAt(0, List.class);
            for (RemoteCorfuTableDatabaseEntry entry : entries) {
                String key = (String) deserializeObject(entry.getKey().getEncodedKey());
                String val = (String) deserializeObject(entry.getValue());
                if (val == null) {
                    database.remove(key);
                } else {
                    database.put(key, val);
                }
            }
            lock.countDown();
            return null;
        }).when(mHandler).updateAll(any(List.class), any(UUID.class));

        doAnswer(invocationOnMock -> {
            database.clear();
            lock.countDown();
            return null;
        }).when(mHandler).clear(any(UUID.class), anyLong());
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
    public void shutdown() throws Exception {
        log.warn("SHUTTING DOWN TEST SERVER");
        table.close();
        logListener.close();
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

    @Test
    public void singleOperationListen() throws InterruptedException {
        lock = new CountDownLatch(1);
        table.insert("Key", "Value");
        logListener.startListening();
        lock.await();
        assertEquals(1, database.size());
        assertTrue(database.containsKey("Key"));
        assertEquals("Value", database.get("Key"));
    }

    @Test
    public void multiOperationListen() throws InterruptedException {
        lock = new CountDownLatch(1000);
        logListener.startListening();
        Map<String, String> expectedMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "Key" + i;
            String val = "Val" + i;
            table.insert(key, val);
            expectedMap.put(key,val);
        }
        lock.await();
        assertEquals(expectedMap, database);
    }

    @Test
    public void multiTableOperationListen() throws Exception {
        lock = new CountDownLatch(5000);
        logListener.startListening();
        table.close();
        Map<String, String> expectedMap = new HashMap<>();
        List<RemoteCorfuTable<String, String>> tableList = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            tableList.add(RemoteCorfuTable.RemoteCorfuTableFactory.openTable(singletonRuntime.get(), "test" + i));
        }
        for (int j = 0; j < 1250; j++) {
            for (int i = 0; i < 4; i++) {
                String key = "Table" + i + "Key" + j;
                String value = "Table" + i + "Val" + j;
                table.insert(key, value);
                expectedMap.put(key, value);
            }
        }
        lock.await();
        assertEquals(expectedMap, database);
    }
}
