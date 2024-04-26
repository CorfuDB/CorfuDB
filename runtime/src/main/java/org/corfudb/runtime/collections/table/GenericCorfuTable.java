package org.corfudb.runtime.collections.table;

import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.SnapshotGenerator;

public interface GenericCorfuTable<S extends SnapshotGenerator<S>, K, V>
        extends ICorfuTable<K, V>, ICorfuSMR<S> {
}
