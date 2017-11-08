package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.CorfuWrapperBuilder;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectBuilder;
import org.corfudb.runtime.object.VersionedObjectManager;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * Created by mwei on 4/6/16.
 */
@Accessors(chain = true)
@Data
@Slf4j
public class ObjectBuilder<T> implements IObjectBuilder<T> {

    @Getter
    final CorfuRuntime runtime;

    @Getter
    Class<T> type;

    @Setter
    @Getter
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

    @Setter
    @Getter
    VersionedObjectManager<T> object;

    @SuppressWarnings("unchecked")
    public <R> ObjectBuilder<R> setType(Class<R> type) {
        this.type = (Class<T>) type;
        return (ObjectBuilder<R>) this;
    }

    @Override
    public UUID getStreamId() {
        return streamID;
    }

    @Override
    public T getRawInstance() {
        try {
            T ret = null;
            if (getArguments() == null || getArguments().length == 0) {
                ret = getType().newInstance();
            } else {
                // This loop is not ideal, but the easiest way to get around Java boxing,
                // which results in primitive constructors not matching.
                for (Constructor<?> constructor : getType().getDeclaredConstructors()) {
                    try {
                        ret = (T) constructor.newInstance(getArguments());
                        break;
                    } catch (Exception e) {
                        // just keep trying until one works.
                    }
                }
            }
            return ret;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
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
                return CorfuWrapperBuilder.getWrapper(this);
            } else {
                ObjectsView.ObjectID<T> oid = new ObjectsView.ObjectID(streamID, type);
                T result = (T) runtime.getObjectsView().objectCache.computeIfAbsent(oid, x -> {
                            try {
                                return CorfuWrapperBuilder.getWrapper(this);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                );
                // Get object serializer to check if we didn't attempt to set another serializer
                // to an already existing map
                ISerializer objectSerializer = ((ObjectBuilder)
                        ((ICorfuWrapper) runtime.getObjectsView().
                        getObjectCache().
                        get(oid)).getObjectManager$CORFU().getBuilder()).getSerializer();

                // FIXME: temporary hack until we have a registry
                // If current map in cache has no indexer, or there is currently an other one,
                // this will create and compute the indices.
                if (result instanceof CorfuTable) {
                    CorfuTable currentCorfuTable = ((CorfuTable) result);
                    if (arguments.length > 0) {
                        // If current map in cache has no indexer, or there is currently an other index
                        if (!(currentCorfuTable.hasSecondaryIndices()) ||
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
