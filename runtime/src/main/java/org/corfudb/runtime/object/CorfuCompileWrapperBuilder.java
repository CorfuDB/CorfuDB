package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;


/**
 * Builds a wrapper for the underlying SMR Object.
 */
public class CorfuCompileWrapperBuilder<T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> {

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
     * @param rt Connected instance of the CorfuRuntime.
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    private T getWrapper(CorfuRuntime rt, SmrObjectConfig<T, S> smrConfig) throws Exception {
        MVOCache<S> mvoCache = rt.getMvoCache(smrConfig.getTableType());
        var smrInstance = smrConfig.newSmrTableInstance();
        MVOCorfuCompileProxy<S> proxy = new MVOCorfuCompileProxy<>(rt, smrConfig, smrInstance, mvoCache);

        smrInstance.setCorfuSMRProxy(proxy);
        return smrInstance;
    }

    public T getWrapper(SMRObject<T, S> smrObject) throws Exception {
        var smrConfig = smrObject.getSmrConfig();
        return getWrapper(smrObject.getRuntime(), smrConfig);
    }
}
