package org.corfudb.runtime.collections.cache;

import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.RocksDbStore.IndexMode;
import org.corfudb.runtime.object.RocksDbStore.StoreMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class ExtensibleCacheTest {

    @Rule
    public TemporaryFolder testTempDir = new TemporaryFolder();

    @Test
    public void testAccess() throws Exception {
        PersistenceOptions persistenceOptions = PersistenceOptions.builder()
                .dataPath(testTempDir.newFolder().toPath())
                .storeMode(StoreMode.PERSISTENT)
                .indexMode(IndexMode.NON_INDEX)
                .build();

        ExtensibleCache<String, String> cache = new ExtensibleCache<>(persistenceOptions);
        cache.put("test", "test");
        cache.close();

        cache = new ExtensibleCache<>(persistenceOptions);
        String value = cache.get("test");
        assertEquals("test", value);
        cache.close();
    }

}