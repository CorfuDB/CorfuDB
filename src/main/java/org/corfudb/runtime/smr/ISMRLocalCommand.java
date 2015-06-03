package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 6/1/15.
 */
@FunctionalInterface
public interface ISMRLocalCommand<T> extends BiConsumer<T, ISMREngine.ISMREngineOptions>, Serializable {
}
