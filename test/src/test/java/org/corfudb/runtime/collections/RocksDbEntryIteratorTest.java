package org.corfudb.runtime.collections;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;

/**
 *
 * Tests that exercise {@link RocksDbEntryIterator} functionality.
 *
 * Created by Maithem on 1/30/20.
 */
public class RocksDbEntryIteratorTest {

    @Test
    public void iteratorTest() throws Exception {
        String path = PARAMETERS.TEST_TEMP_DIR;
        String rocksDbPath = Paths.get(path, "rocksdb").toAbsolutePath().toString();
        final Options options = new Options()
                .setCreateIfMissing(true);
        RocksDB rocksDb = RocksDB.open(options, rocksDbPath);
        ISerializer serializer = Serializers.PRIMITIVE;

        final int numEntries = 57;

        // Generate some data
        IntStream.range(0, numEntries).forEach(num -> {
            try {
                ByteBuf keyByteBuf = Unpooled.buffer();
                ByteBuf valByteBuf = Unpooled.buffer();
                serializer.serialize(num, keyByteBuf);
                serializer.serialize(num, valByteBuf);
                byte[] key = new byte[keyByteBuf.writerIndex()];
                keyByteBuf.readBytes(key);
                byte[] val = Arrays.copyOf(key, key.length);
                rocksDb.put(key, val);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });

        RocksDbEntryIterator iterator = new RocksDbEntryIterator(rocksDb, serializer);

        // Verify that all the entries can be retrieved from the iterator
        IntStream.range(0, numEntries).forEach(num -> {
            Map.Entry<Integer, Integer> entry = iterator.next();
            assertThat(entry.getKey()).isEqualTo(num);
            assertThat(entry.getValue()).isEqualTo(num);
        });

        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(() -> iterator.next())
                .isExactlyInstanceOf(NoSuchElementException.class);
        assertThat(iterator.hasNext()).isFalse();

        // Verify that the iterator can be closed multiple times

        Assertions.assertDoesNotThrow(() -> iterator.close());
        Assertions.assertDoesNotThrow(() -> iterator.close());
    }
}
