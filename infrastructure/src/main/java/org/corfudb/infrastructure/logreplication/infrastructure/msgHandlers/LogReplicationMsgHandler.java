package org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers;

import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
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
     * Returns the request payload type
     * @return the type of log replication request payload
     */
    RequestPayloadMsg.PayloadCase  requestType() default RequestPayloadMsg.PayloadCase.NONE;

    /**
     * Returns the response payload type
     * @return the type of log replication response payload
     */
    ResponsePayloadMsg.PayloadCase  responseType() default ResponsePayloadMsg.PayloadCase.NONE;
}
