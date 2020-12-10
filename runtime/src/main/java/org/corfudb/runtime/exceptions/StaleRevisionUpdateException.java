package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Returns a stale revision transaction abort if the revision of the caller's object
 * does not match with the latest revision value of that record in the database
 *
 * created by hisundar on 2020-09-17
 */
public class StaleRevisionUpdateException extends RuntimeException {
    @Getter
    private final Long correctRevision;

    public StaleRevisionUpdateException(long correctRevision, long givenRevision) {
        super("Update on stale revision. Correct revision = "
                + correctRevision + " Given = " + givenRevision);
        this.correctRevision = correctRevision;
    }
}
