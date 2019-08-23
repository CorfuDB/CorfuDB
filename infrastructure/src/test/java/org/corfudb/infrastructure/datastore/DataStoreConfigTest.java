package org.corfudb.infrastructure.datastore;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.datastore.DataStore.DataStoreConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

class DataStoreConfigTest {

    /**
     * Check correctness of the parsing --memory param
     */
    @Test
    public void testParsingInMem() {
        Map<String, Object> opts = ImmutableMap.of(DataStoreConfig.MEMORY_PARAM, true);
        assertThat(DataStoreConfig.parse(opts).isInMemory()).isEqualTo(true);
    }

    /**
     * Check correctness of the parsing of an empty config
     */
    @Test
    public void testParsingEmptyConfig() {
        Map<String, Object> opts = ImmutableMap.of();
        assertThat(DataStoreConfig.parse(opts).isInMemory()).isEqualTo(true);
    }
}
