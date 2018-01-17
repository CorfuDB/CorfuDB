package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown when a workflow fails.
 * @author Maithem
 */
public class WorkflowException extends RuntimeException {
    public WorkflowException(String msg) {
        super(msg);
    }
}
