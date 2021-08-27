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
import java.util.Observable;
import java.util.Observer;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class RoundRobinListeningTest extends AbstractViewTest {
    private CorfuRuntime runtime;
    private RemoteCorfuTable<String, String> table;
    private RemoteCorfuTableListeningService logListener;
    private LogObserver observer;
    private final ISerializer serializer = Serializers.getDefaultSerializer();
    private final Object lock = new Object();

    private class LogObserver implements Observer {
        @Getter
        private final ConcurrentLinkedQueue<SMROperation> receivedUpdates = new ConcurrentLinkedQueue<>();
        private Predicate<ConcurrentLinkedQueue<SMROperation>> notificationPred = list -> false;

        @Override
        public void update(Observable o, Object arg) {
            assertTrue(o instanceof RoundRobinListeningService);
            assertTrue(arg instanceof SMROperation);
            receivedUpdates.add((SMROperation) arg);
            //System.out.println(receivedUpdates.size());
            notifyIfPred();
        }

        private void notifyIfPred() {
            if (notificationPred.test(receivedUpdates)) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }

        public void registerNotifyCond(Predicate<ConcurrentLinkedQueue<SMROperation>> pred) {
            this.notificationPred = pred;
        }
    }

    @Before
    public void setupTable() throws RocksDBException {
        runtime = getDefaultRuntime();
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test1");
        logListener = new RoundRobinListeningService(Executors.newFixedThreadPool(4), runtime);
        observer = new LogObserver();
        logListener.addObserver(observer);
    }

    @After
    public void shutdownTable() throws Exception {
        table.close();
        logListener.shutdown();
    }

    private void waitForAllUpdatesToPropogate(int desiredNumUpdates) throws InterruptedException {
        final Predicate<ConcurrentLinkedQueue<SMROperation>> endPred = list -> list.size() >= desiredNumUpdates;
        observer.registerNotifyCond(endPred);
        synchronized (lock) {
            while (!endPred.test(observer.getReceivedUpdates())) {
                lock.wait();
            }
        }
    }

    @Test
    public void listenBeforePut() throws InterruptedException {
        logListener.addStream(table.getStreamId());
        table.insert("Key", "Value");
        waitForAllUpdatesToPropogate(1);
        ConcurrentLinkedQueue<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(1, updatesSeen.size());
        SMROperation insertOp = updatesSeen.poll();
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
        table.insert("Key", "Value");
        logListener.addStream(table.getStreamId());
        waitForAllUpdatesToPropogate(1);
        ConcurrentLinkedQueue<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(1, updatesSeen.size());
        SMROperation insertOp = updatesSeen.poll();
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
        waitForAllUpdatesToPropogate(1000);
        ConcurrentLinkedQueue<SMROperation> updatesSeen = observer.getReceivedUpdates();
        assertEquals(keys.size(), updatesSeen.size());
        for (int i = 0; i < 1000; i++) {
            SMROperation insertOp = updatesSeen.poll();
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
                String key = "Table" + i + "Key";
                String val = "Table" + i + "Val" + j;
                tableKeys.get(i).add(key);
                tablePairs.get(i).put(key, val);
                tables.get(i).insert(key, val);
            }
        }
        //waitForAllUpdatesToPropogate(10000);
        Thread.sleep(5000);
        ConcurrentLinkedQueue<SMROperation> updatesSeen = observer.getReceivedUpdates();
        Map<UUID, List<SMROperation>> groupedSMRops = updatesSeen.stream()
                .collect(Collectors.groupingBy(SMROperation::getStreamId));
        for (List<SMROperation> tableGrouped: groupedSMRops.values()) {
            if (tableGrouped.size() < 100) {
                List<String> values = tableGrouped.stream()
                        .map(SMROperation::getEntryBatch)
                        .map(entry -> entry.get(0))
                        .map(RemoteCorfuTableDatabaseEntry::getValue)
                        .map(this::deserializeObject)
                        .map(str -> (String) str)
                        .collect(Collectors.toList());
                int label = 0;
                int pos = 0;
                while (pos < values.size()) {
                    if (!values.get(pos).endsWith("Val" + label)) {
                        System.out.println("Missing label " + label);
                        pos--;
                    }
                    label++;
                    pos++;
                }
                if (label != 100) {
                    System.out.println("Missing all labels " + label + " and above");
                }
            }
        }
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
