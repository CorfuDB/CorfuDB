package org.corfudb.runtime.collections.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.LogListener;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RemoteCorfuTableListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RoundRobinListeningService;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.concurrent.SingletonResource;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class LogListenerTest extends AbstractViewTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private SingletonResource<CorfuRuntime> runtime;
    private RemoteCorfuTable<String, String> table;
    private RemoteCorfuTableListeningService listeningService;
    private LogListener logListener;
    private final ISerializer serializer = Serializers.getDefaultSerializer();
    private CountDownLatch lock;

    //mock database to directly capture puts
    private ConcurrentMap<String, String> database;
    private DatabaseHandler mHandler;

    @Before
    public void setupTable() throws RocksDBException {
        runtime = SingletonResource.withInitial(this::getDefaultRuntime);
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime.get(), "test1");
        listeningService = new RoundRobinListeningService(Executors.newScheduledThreadPool(4),
                runtime, 10);
        mHandler = mock(DatabaseHandler.class);
        logListener = new LogListener(runtime, mHandler, Executors.newScheduledThreadPool(4),
                3, 10, listeningService);
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

    @After
    public void shutdown() throws Exception {
        table.close();
        logListener.close();
        runtime.cleanup(CorfuRuntime::shutdown);
    }

    //taken from RemoteCorfuTableAdapater
    private Object deserializeObject(ByteString serializedObject) {
        if (serializedObject.isEmpty()) {
            return null;
        }
        byte[] deserializationWrapped = new byte[serializedObject.size()];
        serializedObject.copyTo(deserializationWrapped, 0);
        ByteBuf deserializationBuffer = Unpooled.wrappedBuffer(deserializationWrapped);
        return serializer.deserialize(deserializationBuffer, runtime.get());
    }

    @Test
    public void singleOperationListen() throws InterruptedException {
        lock = new CountDownLatch(1);
        table.put("Key", "Value");
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
            table.put(key, val);
            expectedMap.put(key,val);
        }
        lock.await();
        assertEquals(expectedMap, database);
    }

    @Test
    public void multiTableOperationListen() throws Exception {
        lock = new CountDownLatch(10000);
        logListener.startListening();
        table.close();
        Map<String, String> expectedMap = new HashMap<>();
        List<RemoteCorfuTable<String, String>> tableList = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            tableList.add(RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime.get(), "test" + i));
        }
        for (int j = 0; j < 2500; j++) {
            for (int i = 0; i < 4; i++) {
                String key = "Table" + i + "Key" + j;
                String value = "Table" + i + "Val" + j;
                table.put(key, value);
                expectedMap.put(key, value);
            }
        }
        lock.await();
        assertEquals(expectedMap, database);
    }
}
