package org.corfudb.infrastructure.logreplication.exceptions;


/**
 * An exception that is thrown when the message size exceeds the predefined limit
 *
 * Created by Anny on 12/5/22.
 */
public class MessageSizeExceededException extends RuntimeException {
    public MessageSizeExceededException() {
        super("Message size has exceeded the limit set.");
    }

    public MessageSizeExceededException(String msg) {
        super(msg);
    }
}
