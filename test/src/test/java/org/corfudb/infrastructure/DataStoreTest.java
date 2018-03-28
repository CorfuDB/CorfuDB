package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.junit.Test;

import javax.xml.crypto.Data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mdhawan on 7/29/16.
 */
public class DataStoreTest extends AbstractCorfuTest {

    @Test
    public void testPutGet() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
    }

    @Test
    public void testDataCorruption() throws IOException {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);

        String fileName = PARAMETERS.TEST_TEMP_DIR + File.separator + "test_key" + DataStore.EXTENSION;
        RandomAccessFile file1 = new RandomAccessFile(fileName , "rw");

        file1.seek(value.length() / 2);
        file1.writeShort(0);
        file1.close();

        //Simulate a restart of data store
        final DataStore dataStore2 = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        assertThatThrownBy(() -> dataStore2.get(String.class, "test", "key"))
                .isInstanceOf(DataCorruptionException.class);
    }

    @Test
    public void testPutGetWithRestart() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);

        //Simulate a restart of data store
        dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
    }

    @Test
    public void testDatastoreEviction() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(String.class, "test", "key", value);

            //Simulate a restart of data store
            dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                    .put("--log-path", serviceDir)
                    .build());
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);
            dataStore.put(String.class, "test", "key", "NEW_VALUE");
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
        }
    }

    @Test
    public void testInmemoryPutGet() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--memory", true)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");

    }

    @Test
    public void testInmemoryEviction() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--memory", true)
                .build());

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(String.class, "test", "key", value);
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

            dataStore.put(String.class, "test", "key", "NEW_VALUE");
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
        }
    }
}
