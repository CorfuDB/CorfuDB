package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;

import java.util.UUID;

/** The interface to the builder for an object.
 * @param <T> The type of the object to build.
 * Created by mwei on 11/12/16.
 */
public interface IObjectBuilder<T> {

    /** Set the stream ID of the object.
     *
     * @param streamID  The stream ID of the object.
     * @return  This object builder, to support chaining.
     */
    IObjectBuilder<T> setStreamID(UUID streamID);

    /** Set the name of the stream for this object.
     *
     * @param streamName    The stream name for this object.
     * @return  This object builder, to support chaining.
     */
    IObjectBuilder<T> setStreamName(String streamName);

    /** Set the type of this object, using a type token.
     *
     * @param typeToken The type token to use to generate the type.
     * @param <R>   The type of the type token.
     * @return  A typed version of this object builder, to support chaining.
     */
    <R> IObjectBuilder<R> setTypeToken(TypeToken<R> typeToken);

    /** Set the type of this object, using a class.
     *
     * @param type  The class of the object.
     * @param <R>   The type of the class.
     * @return  A typed version of this object builder, to support chaining.
     */
    <R> IObjectBuilder<R> setType(Class<R> type);

    /** Open the object, using the parameters given to the builder.
     *
     * @return  An instance of the Corfu object.
     */
    T open();
}
