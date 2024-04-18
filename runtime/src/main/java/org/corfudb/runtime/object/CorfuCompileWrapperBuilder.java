package org.corfudb.runtime.object;

import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.ImmutableCorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;

import java.util.Set;
import java.util.UUID;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {

    static final String PERSISTENT_CORFU_TABLE_CLASS_NAME = PersistentCorfuTable.class.getName();
    static final String IMMUTABLE_CORFU_TABLE_CLASS_NAME = ImmutableCorfuTable.class.getName();

    static final String PERSISTED_CORFU_TABLE_CLASS_NAME = PersistedCorfuTable.class.getName();
    static final String DISKBACKED_CORFU_TABLE_CLASS_NAME = DiskBackedCorfuTable.class.getName();

    /**
     * Returns a wrapper for the underlying SMR Object
     *
     * @param type       Type of SMR object.
     * @param rt         Connected instance of the CorfuRuntime.
     * @param streamID   StreamID of the SMR Object.
     * @param args       Arguments passed to instantiate the object.
     * @param serializer Serializer to be used to serialize the object arguments.
     * @param <T>        Type
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    private static <T extends ICorfuSMR, S extends SnapshotGenerator<S> & ConsistencyView> T getWrapper(
            Class<T> type, CorfuRuntime rt, UUID streamID, Object[] args,
            ISerializer serializer, Set<UUID> streamTags, ObjectOpenOption objectOpenOption) throws Exception {

        if (type.getName().equals(PERSISTENT_CORFU_TABLE_CLASS_NAME)) {
            // TODO: make general - This should get cleaned up
            Class<S> immutableClass = (Class<S>)
                    Class.forName(IMMUTABLE_CORFU_TABLE_CLASS_NAME);

            Class<ICorfuSMR> wrapperClass = (Class<ICorfuSMR>) Class.forName(type.getName());

            // Instantiate a new instance of this class.
            ICorfuSMR wrapperObject = (ICorfuSMR) ReflectionUtils.
                    findMatchingConstructor(wrapperClass.getDeclaredConstructors(), new Object[0]);

            MVOCache<S> mvoCache = (MVOCache<S>) rt.getObjectsView().getMvoCache();
            // Note: args are used when invoking the internal immutable data structure constructor
            wrapperObject.setCorfuSMRProxy(new MVOCorfuCompileProxy<>(rt, streamID,
                    immutableClass, wrapperClass, args, serializer, streamTags, wrapperObject, objectOpenOption,
                    mvoCache));
            return (T) wrapperObject;
        } else if (type.getName().equals(PERSISTED_CORFU_TABLE_CLASS_NAME)) {
            // TODO: make general - This should also get cleaned up
            Class<S> coreClass = (Class<S>)
                    Class.forName(DISKBACKED_CORFU_TABLE_CLASS_NAME);

            Class<ICorfuSMR> wrapperClass = (Class<ICorfuSMR>) Class.forName(type.getName());

            // Instantiate a new instance of this class.
            ICorfuSMR wrapperObject = (ICorfuSMR) ReflectionUtils.
                    findMatchingConstructor(wrapperClass.getDeclaredConstructors(), new Object[0]);

            // In the context of PersistedCorfuTable, there is one-to-one mapping between
            // the cache and the underlying table/stream. In this case the cache is used to
            // store the underlying Snapshot references and not the data itself. Since
            // there is no contention for the underlying resource (memory), there is no
            // good reason to enforce a global cache.
            MVOCache<S> mvoCache = new MVOCache<>(rt.getParameters().getMvoCacheExpiry());
            wrapperObject.setCorfuSMRProxy(new MVOCorfuCompileProxy<>(rt, streamID,
                    coreClass, wrapperClass, args, serializer, streamTags, wrapperObject, objectOpenOption,
                    mvoCache));

            return ClassUtils.cast(wrapperObject);
        }

        throw new UnsupportedOperationException(type.getName() + " not supported.");
    }

    public static <T extends ICorfuSMR<?>> T getWrapper(SMRObject<T> smrObject) throws Exception {
        return getWrapper(smrObject.getType(),
                smrObject.getRuntime(),
                smrObject.getStreamID(),
                smrObject.getArguments(),
                smrObject.getSerializer(),
                smrObject.getStreamTags(),
                smrObject.getOpenOption());
    }
}
