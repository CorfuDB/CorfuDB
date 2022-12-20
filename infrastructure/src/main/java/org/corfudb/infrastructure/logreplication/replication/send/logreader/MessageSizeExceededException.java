package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.NoArgsConstructor;

/**
 * An exception that is thrown when the message size exceeds the predefined limit
 *
 * Created by Anny on 12/5/22.
 */
@NoArgsConstructor
public class MessageSizeExceededException extends RuntimeException {

    public MessageSizeExceededException(String msg) {
        super("Message size has exceeded the limit set. " + msg);
    }
}
