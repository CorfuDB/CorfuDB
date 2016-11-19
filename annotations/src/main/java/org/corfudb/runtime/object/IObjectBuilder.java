package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;

import java.util.UUID;

/**
 * Created by mwei on 11/12/16.
 */
public interface IObjectBuilder<T> {

    IObjectBuilder<T> setStreamID(UUID streamID);

    IObjectBuilder<T> setStreamName(String streamName);

    <R> IObjectBuilder<R> setTypeToken(TypeToken<R> typeToken);

    <R> IObjectBuilder<R> setType(Class<R> type);

    T open();
}
