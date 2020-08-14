package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


import com.google.common.primitives.UnsignedBytes;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuQueue.CorfuQueueRecord;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

/**
 * Created by Sundar Sridharan on May 22, 2019
 */

/**
 * Simple test of basic operations to check that insert order is preserved in the queue.
 * Created by hisundar on 05/27/2019
 */
@Slf4j
public class CorfuQueueTest extends AbstractViewTest {

    private ByteString getByteString(String string) {
        return ByteString.copyFromUtf8(string);
    }

    @Test
    public void failNonTxnEnqueue() {
        CorfuQueue corfuQueue = new CorfuQueue(getDefaultRuntime(), "test");
        assertThatThrownBy(() -> corfuQueue.enqueue(getByteString("c")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("must be called within a transaction!");
    }

    private void executeTxn(CorfuRuntime rt, Runnable runnable) {
        rt.getObjectsView().TXBegin();
        runnable.run();
        rt.getObjectsView().TXEnd();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void basicQueueOrder() {
        CorfuQueue corfuQueue = new CorfuQueue(getDefaultRuntime(), "test");

        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("c")));
        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("b")));
        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("a")));

        List<CorfuQueueRecord> records = corfuQueue.entryList();

        assertThat(records.get(0).getEntry()).isEqualTo(getByteString("c"));
        assertThat(records.get(1).getEntry()).isEqualTo(getByteString("b"));
        assertThat(records.get(2).getEntry()).isEqualTo(getByteString("a"));

        assertThatThrownBy(() -> corfuQueue.entryList(Integer.MIN_VALUE)).
                isExactlyInstanceOf(IllegalArgumentException.class);

        final int middleEntryIndex = 1;
        // Remove the middle entry
        corfuQueue.removeEntry(corfuQueue.entryList().get(middleEntryIndex).getRecordId());

        List<CorfuQueueRecord> records2 =
                    corfuQueue.entryList(Short.MAX_VALUE);
        assertThat(records2.get(0).getEntry()).isEqualTo(getByteString("c"));
        assertThat(records2.get(1).getEntry()).isEqualTo(getByteString("a"));

        // Also ensure that the records are comparable across snapshots
        assertThat(records.get(0).getRecordId()).
                isLessThan(records2.get(1).getRecordId());

        assertThat(records.get(0).compareTo(records2.get(1))).isLessThan(0);
        assertThat(records.get(0).getRecordId().compareTo(records2.get(1).getRecordId())).isLessThan(0);
    }

    @Test
    public void queueWithSecondaryIndexCheck() {
        CorfuQueue corfuQueue = new CorfuQueue(getDefaultRuntime(), "test");

        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("c")));
        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("b")));
        executeTxn(getDefaultRuntime(), () -> corfuQueue.enqueue(getByteString("a")));

        final int expected = 3;
        List<CorfuQueueRecord> records = corfuQueue.entryList();
        assertThat(records.size()).isEqualTo(expected);

        // Only retrieve entries greater than the first entry.
        List<CorfuQueueRecord> recAfter = corfuQueue.entryList(
                records.get(0).getRecordId(),
                records.size());
        assertThat(recAfter.size()).isEqualTo(records.size() - 1);
    }

    @Test
    public void byteArraylexComparatorCheck() {
        class ByteArray implements Comparable<ByteArray>{
            @Getter
            byte[] bytes;
            Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
            ByteArray(byte[] bytes) { this.bytes = bytes;}

            @Override
            public int compareTo(ByteArray x) {
                return comparator.compare(this.bytes, x.bytes);
            }

            @Override
            public String toString() {
                return bytes.toString();
            }
        }
        Map<ByteArray, String> bmap = new TreeMap<>();
        bmap.put(new ByteArray("xyz".getBytes()), "xyz");
        bmap.put(new ByteArray("lmn".getBytes()), "lmn");
        bmap.put(new ByteArray("fg".getBytes()), "fg");
        bmap.put(new ByteArray("abcd".getBytes()), "abcd");
        for (Map.Entry<ByteArray, String> b : bmap.entrySet()) {
            log.debug("Entry {}", b);
        }
    }

    @Test
    public void queueBackwardsCompatibility() {
        CorfuQueue oldQueueInstance1 = new CorfuQueue(getDefaultRuntime(), "test",
                Serializers.JSON);

        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();
        CorfuQueue oldQueueInstance2 = new CorfuQueue(rt2, "test",
                Serializers.QUEUE_SERIALIZER);

        // produce items to the queue with two different serializers from two different clients
        final int numItemsToProduce = 10;
        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            if (itemIdx % 2 == 0) {
                executeTxn(getDefaultRuntime(),
                        () -> oldQueueInstance1.enqueue(getByteString(String.valueOf(itemIdx))));
            } else {
                executeTxn(rt2,
                        () -> oldQueueInstance2.enqueue(getByteString(String.valueOf(itemIdx))));
            }
        });

        // Verify that a new client is able to see all items produced from the different
        // clients
        CorfuRuntime rt3 = getNewRuntime(getDefaultNode()).connect();
        CorfuQueue newQueueInstance= new CorfuQueue(rt3, "test");

        assertThat(newQueueInstance.size()).isEqualTo(numItemsToProduce);

        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            assertThat(newQueueInstance.entryList().get(itemIdx).getEntry().toStringUtf8())
                    .isEqualTo(String.valueOf(itemIdx));
        });
    }

    @Test
    public void queueMapCompatibility() {

        // Produce some items to the corfu queue
        CorfuQueue queue = new CorfuQueue(getDefaultRuntime(), "test");
        final int numItemsToProduce = 10;
        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            executeTxn(getDefaultRuntime(),
                    () -> queue.enqueue(getByteString(String.valueOf(itemIdx))));
        });

        // Verify that the queue can be opened as a map object
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode()).connect();
        CorfuTable<CorfuQueue.CorfuRecordId, ByteString> map = rt2.getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<CorfuTable<CorfuQueue.CorfuRecordId, ByteString>>() {})
                .setSerializer(Serializers.QUEUE_SERIALIZER)
                .open();

        assertThat(map.size()).isEqualTo(numItemsToProduce);

        Set<String> entryPayloads = map.entryStream()
                .map(Map.Entry::getValue)
                .map(ByteString::toStringUtf8)
                .collect(Collectors.toSet());

        assertThat(entryPayloads.size()).isEqualTo(numItemsToProduce);
        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            assertThat(entryPayloads).contains(String.valueOf(itemIdx));
        });
    }

    @Test
    public void queueCheckpointTest() {

        // Produce to two different queues using the old and new serializer
        CorfuQueue queueInstance1 = new CorfuQueue(getDefaultRuntime(), "stream1",
                Serializers.JSON);

        CorfuQueue queueInstance2 = new CorfuQueue(getDefaultRuntime(), "stream2");

        final int numItemsToProduce = 10;
        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            executeTxn(getDefaultRuntime(),
                    () -> queueInstance1.enqueue(getByteString(String.valueOf(itemIdx))));
            executeTxn(getDefaultRuntime(),
                    () -> queueInstance2.enqueue(getByteString(String.valueOf(itemIdx))));
        });

        // Checkpoint the queues

        CorfuRuntime checkpointerRuntime = getNewRuntime(getDefaultNode())
                .setCacheDisabled(true)
                .connect();

        CorfuTable<CorfuQueue.CorfuRecordId, ByteString> map1 = checkpointerRuntime
                .getObjectsView()
                .build()
                .setStreamName("stream1")
                .setTypeToken(new TypeToken<CorfuTable<CorfuQueue.CorfuRecordId, ByteString>>() {})
                .setSerializer(Serializers.QUEUE_SERIALIZER)
                .open();

        CorfuTable<CorfuQueue.CorfuRecordId, ByteString> map2 = checkpointerRuntime
                .getObjectsView()
                .build()
                .setStreamName("stream2")
                .setTypeToken(new TypeToken<CorfuTable<CorfuQueue.CorfuRecordId, ByteString>>() {})
                .setSerializer(Serializers.QUEUE_SERIALIZER)
                .open();

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map1);
        mcw.addMap(map2);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");
        checkpointerRuntime.getAddressSpaceView().prefixTrim(checkpointAddress);
        checkpointerRuntime.getAddressSpaceView().gc();

        // Verify that after checkpointing and trimming that the queues' data
        // can still be consumed

        CorfuRuntime consumerRuntime = getNewRuntime(getDefaultNode())
                .setCacheDisabled(true)
                .connect();

        CorfuQueue instance1Reader = new CorfuQueue(consumerRuntime, "stream1");
        CorfuQueue instance2Reader = new CorfuQueue(consumerRuntime, "stream2");

        IntStream.range(0, numItemsToProduce).forEach(itemIdx -> {
            assertThat(instance1Reader.entryList().get(itemIdx).getEntry().toStringUtf8())
                    .isEqualTo(String.valueOf(itemIdx));
            assertThat(instance2Reader.entryList().get(itemIdx).getEntry().toStringUtf8())
                    .isEqualTo(String.valueOf(itemIdx));
        });
    }
}
