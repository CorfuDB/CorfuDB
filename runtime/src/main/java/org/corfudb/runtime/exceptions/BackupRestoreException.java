package org.corfudb.runtime.exceptions;

/**
 * Created by George Lu on 11/17/2021
 */
public class BackupRestoreException extends RuntimeException {

    public BackupRestoreException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
