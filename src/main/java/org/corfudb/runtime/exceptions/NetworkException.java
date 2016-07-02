package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 12/14/15.
 */
public class NetworkException extends RuntimeException {

    @Getter
    String endpoint;

    public NetworkException(String message, String endpoint) {
        super(message + " [endpoint=" + endpoint + "]");
        this.endpoint = endpoint;
    }

    public NetworkException(String message, String endpoint, Throwable cause) {
        super(message + " [endpoint=" + endpoint + "]", cause);
        this.endpoint = endpoint;
    }
}
