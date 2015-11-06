package org.corfudb.runtime.exceptions;

import java.io.IOException;

/**
 * Created by mwei on 8/29/15.
 */
public class NonStreamEntryException extends IOException {

    public long address;
    public NonStreamEntryException(String desc, long address)
    {
        super(desc + "[address=" + address + "]");
        this.address = address;
    }
}
