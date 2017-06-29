package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;

import java.util.UUID;

import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
