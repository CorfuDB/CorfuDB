package org.corfudb.runtime;

/**
 * Created by mwei on 4/30/15.
 */
public class HoleEncounteredException extends Exception {
    long address;

    public HoleEncounteredException(long address)
    {
        super("Hole encountered at address " + address);
        this.address = address;
    }
}
