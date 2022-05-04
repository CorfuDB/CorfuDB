package org.corfudb.runtime.collections;

import org.corfudb.runtime.object.ICorfuSMR;

public interface ICorfuImmutable<T extends ICorfuSMR<T>> {

    T getWrapper();
}
