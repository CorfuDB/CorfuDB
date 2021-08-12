package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;

@Slf4j
public class DatabaseComparatorTest {
    static {
        RocksDB.loadLibrary();
    }

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private ReversedVersionedKeyComparator comparator;
    private ComparatorOptions options;

    private final byte[] DEFAULT_VERSION = Longs.toByteArray(0L);
    private final byte[] DEFAULT_KEY = new byte[]{0x00, 0x01, 0x02, 0x03};

    @Before
    public void setup() {
        options = new ComparatorOptions();
        comparator = new ReversedVersionedKeyComparator(options);
    }

    @After
    public void dispose() {
        comparator.close();
        options.close();
    }

    @Test
    public void testNonEqualPrefixComparison() {
        //single byte comparison
        byte[] first = new byte[]{0x01};
        byte[] second = new byte[]{0x00};
        ByteBuffer smallerKey = ByteBuffer.wrap(Bytes.concat(first, DEFAULT_VERSION));
        ByteBuffer largerKey = ByteBuffer.wrap(Bytes.concat(second, DEFAULT_VERSION));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);

        //length comparison after single byte equality
        first = new byte[]{0x00, 0x01};
        second = new byte[]{0x00};
        smallerKey = ByteBuffer.wrap(Bytes.concat(first, DEFAULT_VERSION));
        largerKey = ByteBuffer.wrap(Bytes.concat(second, DEFAULT_VERSION));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);

        //multibyte comparison
        first = new byte[]{0x00, 0x01};
        second = new byte[]{0x00, 0x00};
        smallerKey = ByteBuffer.wrap(Bytes.concat(first, DEFAULT_VERSION));
        largerKey = ByteBuffer.wrap(Bytes.concat(second, DEFAULT_VERSION));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);

        //unsigned comparison
        first = new byte[]{0x00, 0x01, -1};
        second = new byte[]{0x00, 0x01, 0x7f};
        smallerKey = ByteBuffer.wrap(Bytes.concat(first, DEFAULT_VERSION));
        largerKey = ByteBuffer.wrap(Bytes.concat(second, DEFAULT_VERSION));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);
    }

    @Test
    public void testEqualPrefixComparison() {
        //test long comparison basic
        long first = 100;
        long second = 10;
        ByteBuffer smallerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, Longs.toByteArray(first)));
        ByteBuffer largerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, Longs.toByteArray(second)));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);

        //test equal keys
        smallerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, DEFAULT_VERSION));
        largerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, DEFAULT_VERSION));
        assertTrue(comparator.compare(smallerKey, largerKey) == 0);
        assertTrue(comparator.compare(largerKey, smallerKey) == 0);

        //test unsigned comparison
        first = -1;
        second = 1000;
        smallerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, Longs.toByteArray(first)));
        largerKey = ByteBuffer.wrap(Bytes.concat(DEFAULT_KEY, Longs.toByteArray(second)));
        assertTrue(comparator.compare(smallerKey, largerKey) < 0);
        assertTrue(comparator.compare(largerKey, smallerKey) > 0);
    }
}
