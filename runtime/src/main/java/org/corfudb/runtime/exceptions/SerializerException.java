package org.corfudb.runtime.exceptions;

/**
 * Created by annym on 04/12/19
 */
public class SerializerException extends RuntimeException {

    /**
     * Constructor for SerializerException specifying serializer type.
     *
     * @param type serializer type not found for client.
     */
    public SerializerException(Byte type) {
        super("Serializer type code " + type.intValue() + " not found.");
    }

    /**
     * Constructor for SerializerException specifying exception message.
     *
     * @param message exception message.
     */
    public SerializerException(String message) {
        super(message);
    }

    /**
     * Constructor for SerializerException with specified message and cause.
     *
     * @param message exception message
     * @param cause throwable
     */
    public SerializerException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for SerializerException with specified cause.
     *
     * @param cause throwable
     */
    public SerializerException(Throwable cause) {
        super(cause);
    }
}
