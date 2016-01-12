package org.corfudb.runtime.object;

import org.corfudb.runtime.exceptions.UnprocessedException;

import java.lang.annotation.Inherited;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Created by mwei on 1/7/16.
 */
public interface ICorfuSMRObject<T> {
    default T initialObject(Object... arguments) { throw new UnprocessedException(); }
    default T getSMRObject() { throw new UnprocessedException(); }
}
