package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mdhawan on 7/29/16.
 */
public class DataStoreTest extends AbstractCorfuTest {

    @Test
    public void testPutGet() {
        String serviceDir = getTempDir();
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
        String serviceDir = getTempDir();
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
}
