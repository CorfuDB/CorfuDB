package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;


/**
 * Builds a wrapper for the underlying SMR Object.
 */
public class CorfuCompileWrapperBuilder<T extends ICorfuSMR<?>> {

    public enum CorfuTableType {
        PERSISTENT, PERSISTED;

        public static <T extends ICorfuSMR<?>> CorfuTableType parse(TypeToken<T> tableType) {
            if (tableType.getRawType().isAssignableFrom(PersistentCorfuTable.class)) {
                return CorfuTableType.PERSISTENT;
            }

            if (tableType.getRawType().isAssignableFrom(PersistedCorfuTable.class)) {
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
     */
    private T getWrapper(CorfuRuntime rt, SmrObjectConfig<T> smrConfig) {
        MVOCache<?> mvoCache = rt.getMvoCache(smrConfig.getTableType());
        var smrInstance = smrConfig.newSmrTableInstance();
        MVOCorfuCompileProxy<?> proxy = new MVOCorfuCompileProxy<>(rt, smrConfig, smrInstance, ClassUtils.cast(mvoCache));

        smrInstance.setCorfuSMRProxy(ClassUtils.cast(proxy));
        return smrInstance;
    }

    public T getWrapper(SMRObject<T> smrObject) {
        var smrConfig = smrObject.getSmrConfig();
        return getWrapper(smrObject.getRuntime(), smrConfig);
    }
}

