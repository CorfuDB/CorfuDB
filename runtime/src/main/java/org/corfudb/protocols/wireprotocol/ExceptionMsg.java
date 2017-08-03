package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import javax.annotation.Nonnull;
import lombok.Getter;

import org.corfudb.util.serializer.Serializers;

/** This message wraps exceptions that are encountered on the server.
 *
 */
public class ExceptionMsg implements ICorfuPayload<ExceptionMsg> {

    /** The throwable that was thrown remotely. */
    @Getter
    final Throwable throwable;

    /** Constructor for ExceptionMsg.
     *
     * @param t     The throwable to send.
     */
    public ExceptionMsg(Throwable t) {
        throwable = t;
    }

    /** Represents an exception which failed serialization. */
    public static class SerializationFailedException extends Exception {

        /** The name of the class which failed serialization. */
        @Getter
        public final String exceptionClassName;

        /** Constructor for SerializationFailedException.
         *
         * @param exceptionCls  The class which failed serialization.
         */
        public SerializationFailedException(@Nonnull Class<? extends Throwable> exceptionCls) {
            super("Exception " + exceptionCls.getName() + " failed serialization");
            this.exceptionClassName = exceptionCls.getName();
        }
    }

    /** Represents an exception which failed deserialization. */
    public static class DeserializationFailedException extends Exception {

        /** Constructor for deserialization failed exception. */
        public DeserializationFailedException() {
            super("Remote exception failed deserialization");
        }
    }

    /** Generate an exception message from a bytebuf.
     *
     * @param b The buffer to serialize from.
     */
    public ExceptionMsg(ByteBuf b) {
        Throwable t;
        try {
            t = (Throwable) Serializers.JAVA.deserialize(b, null);
        } catch (Exception e) {
            t = new DeserializationFailedException();
        }
        throwable = t;
    }

    /** Serialize an exception message into a bytebuf.
     *
     * @param buf   The buffer to serialize into.
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        try {
            Serializers.JAVA.serialize(throwable, buf);
        } catch (Exception ex) {
            Serializers.JAVA.serialize(new SerializationFailedException(throwable.getClass()), buf);
        }
    }
}
