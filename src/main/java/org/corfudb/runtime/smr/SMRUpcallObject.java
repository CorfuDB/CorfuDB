package org.corfudb.runtime.smr;

/**
 * Created by mwei on 5/29/15.
 */
public class SMRUpcallObject implements ISMRUpcallObject {

    Object o;

    public SMRUpcallObject(Object o)
    {
        this.o = o;
    }
    /**
     * Get the object to be delivered for the upcall.
     *
     * @return The object to be delivered for the upcall.
     */
    @Override
    public Object getObject() {
        return o;
    }
}
