package org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers;

import org.corfudb.runtime.proto.service.CorfuMessage.LogReplicationMsgTypes;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface LogReplicationMsgHandler {

    /**
     * Returns the message payload type
     * @return the type of Corfu message payload
     */
    String type();
}
