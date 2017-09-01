package org.corfudb.annotations;

import org.corfudb.runtime.object.IDirectAccessFunction;

public interface IDirectReadEnum<R> {
    IDirectAccessFunction<R> getDirectFunction();
    String getMutatorName();
}
