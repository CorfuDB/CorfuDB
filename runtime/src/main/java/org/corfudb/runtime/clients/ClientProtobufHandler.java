package org.corfudb.runtime.clients;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.corfudb.runtime.protocol.proto.CorfuProtocol.MessageType;

/**
 * Registers the method with the annotation as a client response
 * handler and invokes on reception of message response.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ClientProtobufHandler {
    /**
     * Returns the message type
     * @return the type of Corfu response message
     */
    MessageType type();
}
