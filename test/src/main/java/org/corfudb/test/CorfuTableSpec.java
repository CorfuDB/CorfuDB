package org.corfudb.test;

import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.table.GenericCorfuTable;

public interface CorfuTableSpec<K extends Message, V extends Message, M extends Message> {
    void test(CorfuTableSpecContext<K, V, M> ctx) throws Exception;

    @AllArgsConstructor
    @Getter
    class CorfuTableSpecContext<K extends Message, V extends Message, M extends Message> {
        private final CorfuRuntime rt;
        private final GenericCorfuTable<?, K, CorfuRecord<V, M>> corfuTable;
    }
}
