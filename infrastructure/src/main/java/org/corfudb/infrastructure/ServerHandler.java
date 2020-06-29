package org.corfudb.infrastructure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;

/**
 * Created by mwei on 8/8/16.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ServerHandler {
    /**
     * Returns the message type
     * @return the type of corfu message
     */
    CorfuMsgType type();
}
