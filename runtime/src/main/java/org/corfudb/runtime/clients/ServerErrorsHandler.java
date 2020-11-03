package org.corfudb.runtime.clients;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;

/**
 * Registers the method with the annotation as a server error
 * handler and invokes on reception of error response.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ServerErrorsHandler {
    /**
     * Returns the error type
     * @return the type of Corfu server error.
     */
    ErrorCase type();
}
