package org.corfudb.runtime.clients;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;

/**
 * Registers the method with the annotation as a client response
 * handler and invokes on reception of message response.
 *
 * <p>Created by mwei on 8/9/16.
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ClientHandler {

    /**
     * Type of CorfuMsg
     *
     * @return Returns the type of the corfu message.
     */
    CorfuMsgType type();
}
