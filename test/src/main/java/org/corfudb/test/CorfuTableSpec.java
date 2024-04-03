package org.corfudb.test;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.table.GenericCorfuTable;

public interface CorfuTableSpec<K, V> {
    void test(CorfuTableSpecContext<K, V> ctx) throws Exception;

    @AllArgsConstructor
    @Getter
    class CorfuTableSpecContext<K, V> {
        private final CorfuRuntime rt;
        private final GenericCorfuTable<?, K, V> corfuTable;
    }
}
