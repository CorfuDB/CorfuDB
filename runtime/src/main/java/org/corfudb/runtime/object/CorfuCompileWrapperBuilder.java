package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.ImmutableCorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;
import org.corfudb.util.ReflectionUtils;


/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {

    public enum CorfuTableType {
        PERSISTENT, PERSISTED;

        public static <T extends ICorfuSMR<?>> CorfuTableType parse(Class<T> tableType) {
            if (tableType.isAssignableFrom(PersistentCorfuTable.class)) {
                return CorfuTableType.PERSISTENT;
            }

            if (tableType.isAssignableFrom(PersistedCorfuTable.class)) {
                return CorfuTableType.PERSISTED;
            }

            throw new IllegalArgumentException("Unknown table type");
        }
    }

    /**
     * Returns a wrapper for the underlying SMR Object
     *
     * @param rt         Connected instance of the CorfuRuntime.
     * @param <T>        Type
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    private static <T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView>
    T getWrapper(CorfuRuntime rt, SmrObjectConfig<T, S> smrConfig) throws Exception {

        MVOCache<S> mvoCache;
        switch (smrConfig.getTableType()) {
            case PERSISTENT:
                mvoCache = ClassUtils.cast(rt.getObjectsView().getMvoCache());
                break;
            case PERSISTED:
                // In the context of PersistedCorfuTable, there is one-to-one mapping between
                // the cache and the underlying table/stream. In this case the cache is used to
                // store the underlying Snapshot references and not the data itself. Since
                // there is no contention for the underlying resource (memory), there is no
                // good reason to enforce a global cache.
                mvoCache = new MVOCache<>(rt.getParameters().getMvoCacheExpiry());
                break;
            default:
                throw new UnsupportedOperationException(smrConfig.getType().getName() + " not supported.");
        }


        var smrInstance = smrConfig.newSmrTableInstance();
        MVOCorfuCompileProxy<S> proxy = new MVOCorfuCompileProxy<>(rt, smrConfig, smrInstance, mvoCache);

        smrInstance.setCorfuSMRProxy(proxy);
        return smrInstance;
    }

    public static <T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView>
    T getWrapper(SMRObject<T> smrObject) throws Exception {
        var smrConfig = smrObject.getSmrConfig();
        return getWrapper(smrObject.getRuntime(), smrConfig);
    }
}
