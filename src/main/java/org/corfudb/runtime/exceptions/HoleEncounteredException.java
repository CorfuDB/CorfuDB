package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.runtime.stream.ITimestamp;

/**
 * Created by mwei on 4/30/15.
 */
public class HoleEncounteredException extends Exception {

    @Getter
    ITimestamp address;

    public HoleEncounteredException(ITimestamp address)
    {
        super("Hole encountered at address " + address);
        this.address = address;
    }
}
