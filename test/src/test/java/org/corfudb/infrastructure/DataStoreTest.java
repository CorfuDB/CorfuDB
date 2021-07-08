package org.corfudb.infrastructure;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mdhawan on 7/29/16.
 */
public class DataStoreTest extends AbstractCorfuTest {

    private static final KvRecord<String> TEST_RECORD = KvRecord.of("test", "key", String.class);

    private DataStore createPersistDataStore(String serviceDir, int numRetention,
                                             Consumer<String> cleanupTask) {
        return new DataStore(new ServerConfiguration().setServerDirectory(serviceDir).setMetadataRetention(numRetention), cleanupTask);
    }

    private DataStore createInMemoryDataStore() {
        return new DataStore(new ServerConfiguration().setInMemoryMode(true), fn -> {});
    }

    @Test
    public void testPutGet() {
        final int numRetention = 10;
        DataStore dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
        String value = UUID.randomUUID().toString();
        dataStore.put(TEST_RECORD, value);
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);

        dataStore.delete(TEST_RECORD);
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);

        dataStore.put(TEST_RECORD, "NEW_VALUE");
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo("NEW_VALUE");
    }

    @Test
    public void testDataCorruption() throws IOException {
        final int numRetention = 10;
        DataStore dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
        String value = UUID.randomUUID().toString();
        dataStore.put(TEST_RECORD, value);

        String fileName = dataStore.getLogDirPath() + File.separator + "test_key" + DataStore.EXTENSION;
        RandomAccessFile dsFile = new RandomAccessFile(fileName, "rw");

        dsFile.seek(value.length() / 2);
        dsFile.writeShort(0);
        dsFile.close();

        // Simulate a restart of data store
        final DataStore dataStore2 = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
        assertThatThrownBy(() -> dataStore2.get(TEST_RECORD))
                .isInstanceOf(DataCorruptionException.class);
    }

    @Test
    public void testPutGetWithRestart() {
        final int numRetention = 10;
        DataStore dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
        String value = UUID.randomUUID().toString();
        dataStore.put(TEST_RECORD, value);

        // Simulate a restart of data store
        dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);

        dataStore.put(TEST_RECORD, "NEW_VALUE");
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo("NEW_VALUE");
    }

    @Test
    public void testDataStoreEviction() {
        final int numRetention = 10;
        DataStore dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(TEST_RECORD, value);

            // Simulate a restart of data store
            dataStore = createPersistDataStore(PARAMETERS.TEST_TEMP_DIR, numRetention, fn -> {});
            assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);
            dataStore.put(TEST_RECORD, "NEW_VALUE");
            assertThat(dataStore.get(TEST_RECORD)).isEqualTo("NEW_VALUE");
        }
    }

    @Test
    public void testDataStoreCleanup() {
        final int numRetention = 10;

        ServerContext serverContext = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setRetention(numRetention)
                .build();
        DataStore dataStore = serverContext.getDataStore();
        Set<String> prefixesToClean = serverContext.getDsFilePrefixesForCleanup();
        File serviceDir = new File(serverContext.getConfiguration().getServerDir());

        for (int i = 1; i < numRetention + 2; i++) {
            final int epoch = i;
            prefixesToClean.forEach(prefix -> {
                String key = epoch + "KEY";
                String value = UUID.randomUUID().toString();
                dataStore.put(KvRecord.of(prefix, key, String.class), value);
            });

            prefixesToClean.forEach(prefix -> {
                // Cleanup should not be invoked for the first numRetention files,
                // but start to delete files with smaller epochs after that
                File[] foundFiles = serviceDir.listFiles((dir, name) -> name.startsWith(prefix));
                if (epoch > numRetention) {
                    assertThat(foundFiles).hasSize(numRetention);
                    // Check the numRetention files with larger epochs are not deleted
                    for (int j = epoch; j > epoch - numRetention; j--) {
                        String fileName = prefix + "_" + epoch + "KEY" + DataStore.EXTENSION;
                        assertThat(new File(serviceDir, fileName).exists()).isTrue();
                    }
                } else {
                    assertThat(foundFiles).hasSize(epoch);
                }
            });
        }
    }

    @Test
    public void testInMemoryPutGet() {
        DataStore dataStore = createInMemoryDataStore();
        String value = UUID.randomUUID().toString();
        dataStore.put(TEST_RECORD, value);
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);

        dataStore.put(TEST_RECORD, "NEW_VALUE");
        assertThat(dataStore.get(TEST_RECORD)).isEqualTo("NEW_VALUE");

    }

    @Test
    public void testInMemoryEviction() {
        DataStore dataStore = createInMemoryDataStore();

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(TEST_RECORD, value);
            assertThat(dataStore.get(TEST_RECORD)).isEqualTo(value);

            dataStore.put(TEST_RECORD, "NEW_VALUE");
            assertThat(dataStore.get(TEST_RECORD)).isEqualTo("NEW_VALUE");
        }
    }
}
