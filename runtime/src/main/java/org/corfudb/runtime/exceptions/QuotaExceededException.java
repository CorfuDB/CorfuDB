package org.corfudb.runtime.exceptions;


import lombok.NoArgsConstructor;

/**
 * An exception that is thrown when the quota of a resource has been exhausted
 *
 * Created by Maithem on 6/20/19.
 */
@NoArgsConstructor
public class QuotaExceededException extends RuntimeException {
    public QuotaExceededException(String msg) {
        super("Disk usage has exceeded the quota set, system is now in read-only mode. "
                + msg);
    }
}