package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Disk-backed {@link StreamingMap} tests.
 */
public class DiskBackedCorfuClientTest extends AbstractViewTest {

    /**
     * Single type POJO serializer.
     */
    public static class PojoSerializer implements ISerializer {
        private final Gson gson = new Gson();
        private final Class<?> clazz;
        private final int SERIALIZER_OFFSET = 23;  // Random number.

        PojoSerializer(Class<?> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte getType() {
            return SERIALIZER_OFFSET;
        }

        @Override
        public Object deserialize(ByteBuf b, CorfuRuntime rt) {
            return gson.fromJson(new String(PersistedStreamingMap.byteArrayFromBuf(b)), clazz);
        }

        @Override
        public void serialize(Object o, ByteBuf b) {
            b.writeBytes(gson.toJson(o).getBytes());
        }
    }

    /**
     * Sample POJO class.
     */
    @Data
    @Builder
    public static class Pojo {
        public final String payload;
    }

    /**
     * Ensure disk-backed table serialization and deserialization works as expected.
     *
     * @throws RocksDBException
     */
    @Test
    public void customSerializer() throws RocksDBException {
        RocksDB.loadLibrary();

        final File persistedCacheLocation = new File("/tmp/", "diskBackedMap");
        final Options options =
                new Options().setCreateIfMissing(true)
                        // The size is checked either during flush or compaction.
                        .setWriteBufferSize(FileUtils.ONE_KB);

        CorfuTable<String, Pojo>
                table = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, Pojo>>() {})
                .setArguments(new PersistedStreamingMap<String, Pojo>(
                        persistedCacheLocation, options,
                        new PojoSerializer(Pojo.class), getRuntime()))
                .setStreamName("diskBackedMap")
                .open();

        final long ITERATION_COUNT = 100;
        final int ENTITY_CHAR_SIZE = 100;

        LongStream.rangeClosed(1, ITERATION_COUNT).forEach(idx -> {
            String key = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
            Pojo value = Pojo.builder()
                    .payload(RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true))
                    .build();
            table.put(key, value);
            Pojo persistedValue = table.get(key);
            Assertions.assertEquals(value, persistedValue);
        });
    }

    /**
     * Ensure that file-system quota is obeyed.
     *
     * @throws RocksDBException
     */
    @Test
    public void fileSystemLimit() throws RocksDBException {
        RocksDB.loadLibrary();

        final File persistedCacheLocation = new File("/tmp/", "diskBackedMap");
        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());
        sstFileManager.setMaxAllowedSpaceUsage(FileUtils.ONE_KB);
        sstFileManager.setCompactionBufferSize(FileUtils.ONE_KB);

        final Options options =
                new Options().setCreateIfMissing(true)
                        .setSstFileManager(sstFileManager)
                        // The size is checked either during flush or compaction.
                        .setWriteBufferSize(FileUtils.ONE_KB);

        CorfuTable<String, String>
                table = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(new PersistedStreamingMap<String, String>(
                        persistedCacheLocation, options,
                        Serializers.JSON, getRuntime()))
                .setStreamName("diskBackedMap")
                .open();

        final long ITERATION_COUNT = 100000;
        final int ENTITY_CHAR_SIZE = 1000;

        assertThatThrownBy(() ->
                LongStream.rangeClosed(1, ITERATION_COUNT).forEach(idx -> {
                    String key = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
                    String value = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
                    table.put(key, value);
                    String persistedValue = table.get(key);
                    Assertions.assertEquals(value, persistedValue);
                })).isInstanceOf(UnrecoverableCorfuError.class)
                .hasCauseInstanceOf(RocksDBException.class);
    }

    /**
     * Ensure that scan-and-filter works as expected.
     *
     * @throws RocksDBException
     */
    @Test
    public void scanAndFilter() throws RocksDBException {
        RocksDB.loadLibrary();

        final File persistedCacheLocation = new File("/tmp/", "diskBackedMap");
        final Options options = new Options().setCreateIfMissing(true);
        final StreamingMap streamingMap = new PersistedStreamingMap<String, String>(
                persistedCacheLocation, options,
                new PojoSerializer(String.class), getRuntime());
        CorfuTable<String, String>
                table = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(streamingMap)
                .setStreamName("diskBackedMap")
                .open();

        final long ITERATION_COUNT = 100;
        final int ENTITY_CHAR_SIZE = 100;

        Map<String, String> correctMap = new HashMap<String, String>();
        LongStream.rangeClosed(1, ITERATION_COUNT).forEach(idx -> {
            String key = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
            String value = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
            Assertions.assertNotNull(key);
            Assertions.assertNotNull(value);
            table.put(key, value);
            table.get(key).equals(value);
            correctMap.put(key, value);
        });

        Assertions.assertEquals(correctMap.size(), table.entryStream().count());
        table.entryStream().forEach(Assertions::assertNotNull);
        table.entryStream().map(Map.Entry::getKey).map(correctMap::remove)
                .forEach(Assertions::assertNotNull);
        Assertions.assertEquals(0, correctMap.size());
    }
}
