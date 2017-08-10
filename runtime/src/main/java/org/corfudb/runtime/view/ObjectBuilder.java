package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.IObjectBuilder;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * Created by mwei on 4/6/16.
 */
@Accessors(chain = true)
@Data
@Slf4j
public class ObjectBuilder<T> implements IObjectBuilder<T> {

    final CorfuRuntime runtime;

    Class<T> type;

    @Setter
    @SuppressWarnings("checkstyle:abbreviation")
    UUID streamID;

    @Setter
    String streamName;

    @Setter
    ISerializer serializer = Serializers.JSON;

    @Setter
    Set<ObjectOpenOptions> options = EnumSet.noneOf(ObjectOpenOptions.class);

    @Setter(AccessLevel.NONE)
    Object[] arguments = new Object[0];

    @SuppressWarnings("unchecked")
    public <R> ObjectBuilder<R> setType(Class<R> type) {
        this.type = (Class<T>) type;
        return (ObjectBuilder<R>) this;
    }

    @SuppressWarnings("unchecked")
    public <R> ObjectBuilder<R> setTypeToken(TypeToken<R> typeToken) {
        this.type = (Class<T>)typeToken.getRawType();
        return (ObjectBuilder<R>) this;
    }

    /**
     * Add option to object builder.
     *
     * @param option object builder open option (e.g., No-Cache).
     */
    public ObjectBuilder<T> addOption(ObjectOpenOptions option) {
        if (options == null) {
            options = EnumSet.noneOf(ObjectOpenOptions.class);
        }
        options.add(option);
        return this;
    }

    public ObjectBuilder<T> setArguments(Object... arguments) {
        this.arguments = arguments;
        return this;
    }

    public ObjectBuilder<T> setArgumentsArray(Object[] arguments) {
        this.arguments = arguments;
        return this;
    }

    /**
     * Open an Object.
     */
    @SuppressWarnings("unchecked")
    public T open() {

        if (streamName != null) {
            streamID = CorfuRuntime.getStreamID(streamName);
        }

        try {
            if (options.contains(ObjectOpenOptions.NO_CACHE)) {
                return CorfuCompileWrapperBuilder.getWrapper(type, runtime, streamID,
                        arguments, serializer);
            } else {
                ObjectsView.ObjectID<T> oid = new ObjectsView.ObjectID(streamID, type);
                T result = (T) runtime.getObjectsView().objectCache.computeIfAbsent(oid, x -> {
                            try {
                                return CorfuCompileWrapperBuilder.getWrapper(type, runtime,
                                        streamID, arguments, serializer);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                );
                // Get object serializer to check if we didn't attempt to set another serializer
                // to an already existing map
                ISerializer objectSerializer = ((CorfuCompileProxy) ((ICorfuSMR) runtime.getObjectsView().
                        getObjectCache().
                        get(oid)).
                        getCorfuSMRProxy())
                        .getSerializer();

                // FIXME: temporary hack until we have a registry
                // If current map in cache has no indexer, or there is currently an other one,
                // this will create and compute the indices.
                if (result instanceof CorfuTable) {
                    CorfuTable currentCorfuTable = ((CorfuTable) result);
                    if (arguments.length > 0) {
                        // If current map in cache has no indexer, or there is currently an other index
                        if (!(currentCorfuTable.hasIndices()) ||
                            currentCorfuTable.getIndexerClass() != arguments[0].getClass()){
                            ((CorfuTable) result).registerIndex((Class) arguments[0]);
                        }
                    }
                }

                if (serializer != objectSerializer) {
                    log.warn("open: Attempt to open an existing object with a different serializer {}. " +
                            "Object {} opened with original serializer {}.",
                            serializer.getClass().getSimpleName(),
                            oid,
                            objectSerializer.getClass().getSimpleName());
                }
                return result;
            }
        } catch (Exception ex) {
            log.error("Runtime instrumentation no longer supported and no compiled class found"
                    + " for {}", type);
            throw new RuntimeException(ex);
        }
    }


}
