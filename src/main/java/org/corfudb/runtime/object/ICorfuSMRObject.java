package org.corfudb.runtime.object;

import org.corfudb.runtime.exceptions.UnprocessedException;

import java.lang.annotation.Inherited;

/**
 * Created by mwei on 1/7/16.
 */
public interface ICorfuSMRObject<T> {
    default T initialObject(Object... arguments) { throw new UnprocessedException(); }
    default T getSMRObject() { throw new UnprocessedException(); }
}
