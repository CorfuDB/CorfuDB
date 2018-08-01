package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 12/14/15.
 */
@Getter
public class OverwriteException extends LogUnitException {
   private OverwriteCause overWriteCause;

    public OverwriteException(OverwriteCause cause) {
        this.overWriteCause = cause;
    }
}
