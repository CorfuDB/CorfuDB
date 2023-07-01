package org.corfudb.infrastructure;

import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface RequestHandler {
    /**
     * Returns the request message payload type
     * @return the type of Corfu message request
     */
    PayloadCase type();
}
