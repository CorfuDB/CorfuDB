package org.corfudb.runtime.smr;

/**
 * Created by mwei on 5/29/15.
 */
public interface ISMRUpcallObject {
    /**
     * Get the object to be delivered for the upcall.
     * @return  The object to be delivered for the upcall.
     */
    Object getObject();
}
