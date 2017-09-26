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

    /** Get the type set for this object.
     * @return  The type for this object.
     */
    Class<T> getType();

    /** Get the arguments used to construct this object.
     * @return  The arguments used for this object.
     */
    Object[] getArguments();

    /** Get the stream ID for this object.
     * @return  The stream ID for this object.
     */
    UUID getStreamId();

    /** Get a new instance of this object.
     * @return  Get a new uninstrumented instance of this object.
     */
    T getRawInstance();

    /** Open the object, using the parameters given to the builder.
     *
     * @return  An instance of the Corfu object.
     */
    T open();
}
