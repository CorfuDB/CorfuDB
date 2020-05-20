package org.corfudb.utils.common;

/**
 * Exception thrown when there is an error handling a ProtoBuf message.
 *
 * @author annym
 */
public class CorfuMessageProtoBufException extends Throwable {

    public CorfuMessageProtoBufException(Exception e) {
        super(e);
    }
}
