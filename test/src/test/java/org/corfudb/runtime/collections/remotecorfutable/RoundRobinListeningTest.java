package org.corfudb.runtime.collections.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RemoteCorfuTableListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.RoundRobinListeningService;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class RoundRobinListeningTest extends AbstractViewTest {
    private CorfuRuntime runtime;
    private RemoteCorfuTable<String, String> table;
    private RemoteCorfuTableListeningService logListener;
    private LogObserver observer;
    private final ISerializer serializer = Serializers.getDefaultSerializer();
    private CountDownLatch lock;

    private class LogObserver {
        @Getter
        private final List<SMROperation> receivedUpdates = new LinkedList<>();

        public void start() {
            CompletableFuture.runAsync(this::pollTask);
        }

        private void pollTask() {
            SMROperation op = logListener.getTask();
            if (op != null) {
                receivedUpdates.add(op);
                lock.countDown();
            }
            CompletableFuture.runAsync(this::pollTask);
        }
    }

    @Before
    public void setupTable() throws RocksDBException {
        runtime = getDefaultRuntime();
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test1");
        logListener = new RoundRobinListeningService(Executors.newScheduledThreadPool(4), runtime, 10);
        observer = new LogObserver();
        observer.start();
    }

    @After
    public void shutdownTable() throws Exception {
        table.close();
        logListener.shutdown();
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
        lock = new CountDownLatch(10000);
        List<RemoteCorfuTable<String, String>> tables = new ArrayList<>(100);
        List<List<String>> tableKeys = new ArrayList<>(100);
        List<Map<String, String>> tablePairs = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            tables.add(RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "table" + i));
            logListener.addStream(tables.get(i).getStreamId());
            tableKeys.add(new ArrayList<>(100));
            tablePairs.add(new HashMap<>(100));
        }

        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 100; i++) {
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
        for (int i = 0; i < 100; i++) {
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
        return serializer.deserialize(deserializationBuffer, runtime);
    }

}
