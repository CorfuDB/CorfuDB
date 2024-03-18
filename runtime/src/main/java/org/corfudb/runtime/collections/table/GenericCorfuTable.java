package org.corfudb.runtime.collections.table;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.SnapshotGenerator;

public interface GenericCorfuTable<S extends SnapshotGenerator<S> & ConsistencyView, K, V>
        extends ICorfuTable<K, V>, ICorfuSMR<S> {

    TypeToken<S> getTableTypeToken();
}
