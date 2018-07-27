package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown when the result of a workflow is unknown
 * @author Maithem
 */
public class WorkflowResultUnknownException extends RuntimeException {

    public WorkflowResultUnknownException() {
    }

    public WorkflowResultUnknownException(String message) {
        super(message);
    }

    public WorkflowResultUnknownException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkflowResultUnknownException(Throwable cause) {
        super(cause);
    }

    public WorkflowResultUnknownException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}