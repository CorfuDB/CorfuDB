package org.corfudb.runtime.object;

import java.util.Map;
import java.util.UUID;

/** The interface for an object interfaced with SMR.
 *
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMR<T> {

    String CORFUSMR_SUFFIX = "$CORFUSMR";

    ICorfuSMRProxy<T> getCorfuSMRProxy();

    void setCorfuSMRProxy(ICorfuSMRProxy<T> proxy);

    Map<String, ICorfuSMRUpcallTarget<T>> getCorfuSMRUpcallMap();

    default UUID getCorfuStreamID() {
        return getCorfuSMRProxy().getStreamID();
    }
}
