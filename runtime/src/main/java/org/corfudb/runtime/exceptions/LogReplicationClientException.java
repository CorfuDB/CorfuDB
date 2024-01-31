package org.corfudb.runtime.exceptions;

public class LogReplicationClientException extends RuntimeException {
        public LogReplicationClientException(String message) {
            super(message);
        }

        public LogReplicationClientException(String message, Throwable cause) {
            super(message, cause);
        }

        public LogReplicationClientException(Throwable cause) {
            super(cause);
        }
}
