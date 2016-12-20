package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.Constructor;
import java.util.UUID;

/**
 * Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {

    @SuppressWarnings("unchecked")
    public static <T> T getWrapper(Class<T> type, CorfuRuntime rt,
                                   UUID streamID, Object[] args,
                                   ISerializer serializer)
        throws ClassNotFoundException, IllegalAccessException,
            InstantiationException {
        // Do we have a compiled wrapper for this type?
        Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> wrapperObject = null;
        if (args == null || args.length == 0) {
            wrapperObject = wrapperClass.newInstance();
        }
        else {
            // This loop is not ideal, but the easiest way to get around Java
            // boxing, which results in primitive constructors not matching.
            for (Constructor<?> constructor : wrapperClass
                    .getDeclaredConstructors()) {
                try {
                    wrapperObject = (ICorfuSMR<T>) constructor
                            .newInstance(args);
                    break;
                } catch (Exception e) {
                    // just keep trying until one works.
                }
            }
        }

        // Now we create the proxy, which actually manages
        // instances of this object. The wrapper delegates calls to the proxy.
        wrapperObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, streamID,
                type, args, serializer,
                wrapperObject.getCorfuSMRUpcallMap(),
                wrapperObject.getCorfuUndoMap(),
                wrapperObject.getCorfuUndoRecordMap()));

        if (wrapperObject instanceof ICorfuSMRProxyWrapper) {
            ((ICorfuSMRProxyWrapper) wrapperObject)
                    .setProxy$CORFUSMR(wrapperObject.getCorfuSMRProxy());
        }

        return (T) wrapperObject;
    }
}
