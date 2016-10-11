package org.corfudb.runtime.clients;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;

import java.lang.annotation.*;

/**
 * Created by mwei on 8/9/16.
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ClientHandler {
    CorfuMsgType type();
}
