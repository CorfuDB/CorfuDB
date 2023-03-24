package org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers;

import org.corfudb.runtime.proto.service.CorfuMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface LogReplicationResponseHandler {
    /**
     * Returns the response payload type
     * @return the type of log replication response payload
     */
    CorfuMessage.ResponsePayloadMsg.PayloadCase  responseType();
}
