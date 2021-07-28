package org.corfudb.infrastructure.remotecorfutable;

/**
 * This class defines the Exception format for errors arising in RemoteCorfuTable database operations.
 *
 * <p>Created by nvaishampayan517 on 7/27/21.
 */
public class DatabaseOperationException extends Exception {
    public DatabaseOperationException(String operation, String reason) {
        super(String.format("Remote Corfu Table Operation %s failed due to %s", operation, reason));
    }
}
