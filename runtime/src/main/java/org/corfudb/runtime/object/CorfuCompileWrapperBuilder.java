package org.corfudb.runtime.object;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {


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
    public static <T> T getWrapper(Class<T> type, CorfuRuntime rt,
                                   UUID streamID, Object[] args,
                                   ISerializer serializer)
            throws Exception {
        // Do we have a compiled wrapper for this type?
        Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> wrapperObject = (ICorfuSMR<T>) ReflectionUtils.
                findMatchingConstructor(wrapperClass.getDeclaredConstructors(), new Object[0]);

        // Now we create the proxy, which actually manages
        // instances of this object. The wrapper delegates calls to the proxy.
        wrapperObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, streamID,
                type, args, serializer,
                wrapperObject.getCorfuSMRUpcallMap(),
                wrapperObject.getCorfuUndoMap(),
                wrapperObject.getCorfuUndoRecordMap(),
                wrapperObject.getCorfuResetSet()));

        if (wrapperObject instanceof ICorfuSMRProxyWrapper) {
            ((ICorfuSMRProxyWrapper) wrapperObject)
                    .setProxy$CORFUSMR(wrapperObject.getCorfuSMRProxy());
        }

        return (T) wrapperObject;
    }
}
