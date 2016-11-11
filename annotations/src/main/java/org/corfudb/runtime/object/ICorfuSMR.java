package org.corfudb.runtime.object;

/**
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMR<T> {
    ICorfuSMRProxy<T> getCorfuSMRProxy();
}
