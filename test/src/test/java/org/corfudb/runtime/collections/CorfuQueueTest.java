package org.corfudb.runtime.collections;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import lombok.Getter;
import org.corfudb.runtime.collections.CorfuQueue.CorfuQueueRecord;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Sundar Sridharan on May 22, 2019
 */
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Simple test of basic operations to check that insert order is preserved in the queue.
 * Created by hisundar on 05/27/2019
 */
public class CorfuQueueTest extends AbstractViewTest {

    @Test
    @SuppressWarnings("unchecked")
    public void basicQueueOrder() {
        CorfuQueue<String>
                corfuQueue = new CorfuQueue<>(getDefaultRuntime(), "test");

        corfuQueue.enqueue(ByteString.copyFromUtf8("c"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("b"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("a"));

        List<CorfuQueueRecord<ByteString>> records = corfuQueue.entryList();

        assertThat(records.get(0).getEntry()).isEqualTo(ByteString.copyFromUtf8("c"));
        assertThat(records.get(1).getEntry()).isEqualTo(ByteString.copyFromUtf8("b"));
        assertThat(records.get(2).getEntry()).isEqualTo(ByteString.copyFromUtf8("a"));

        assertThatThrownBy(() -> corfuQueue.entryList(Integer.MIN_VALUE)).
                isExactlyInstanceOf(IllegalArgumentException.class);

        final int middleEntryIndex = 1;
        // Remove the middle entry
        corfuQueue.removeEntry(corfuQueue.entryList().get(middleEntryIndex).getRecordId());

        List<CorfuQueueRecord<ByteString>> records2 =
                    corfuQueue.entryList(Short.MAX_VALUE);
        assertThat(records2.get(0).getEntry()).isEqualTo("c");
        assertThat(records2.get(1).getEntry()).isEqualTo("a");

        // Also ensure that the records are comparable across snapshots
        assertThat(records.get(0).getRecordId()).
                isLessThan(records2.get(1).getRecordId());

        assertThat(records.get(0).compareTo(records2.get(1))).isLessThan(0);
        assertThat(records.get(0).getRecordId().compareTo(records2.get(1).getRecordId())).isLessThan(0);
    }

    @Test
    public void queueWithSecondaryIndexCheck() {
        CorfuQueue<ByteString>
                corfuQueue = new CorfuQueue<>(getDefaultRuntime(), "test", Serializers.JAVA,
                Index.Registry.empty());

        corfuQueue.enqueue(ByteString.copyFromUtf8("c"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("b"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("a"));

        final int expected = 3;
        List<CorfuQueueRecord<ByteString>> records = corfuQueue.entryList();
        assertThat(records.size()).isEqualTo(expected);

        // Only retrieve entries greater than the first entry.
        List<CorfuQueueRecord<ByteString>> recAfter = corfuQueue.entryList(
                records.get(0).getRecordId(),
                records.size());
        assertThat(recAfter.size()).isEqualTo(records.size() - 1);
    }

    @Test
    public void queueWithByteString() {
        CorfuQueue<ByteString>
                corfuQueue = new CorfuQueue<>(getDefaultRuntime(), "test");

        corfuQueue.enqueue(ByteString.copyFromUtf8("A"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("B"));
        corfuQueue.enqueue(ByteString.copyFromUtf8("C"));

        final int expected = 3;
        List<CorfuQueueRecord<ByteString>> records = corfuQueue.entryList();
        assertThat(records.size()).isEqualTo(expected);

        // Only retrieve entries greater than the first entry.
        List<CorfuQueueRecord<ByteString>> recAfter = corfuQueue.entryList(
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
            System.out.println(b);
        }


    }
}
