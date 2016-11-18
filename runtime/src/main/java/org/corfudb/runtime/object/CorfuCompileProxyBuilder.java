package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 11/11/16.
 */
public class CorfuCompileProxyBuilder {

    @SuppressWarnings("unchecked")
    public static <T> T getProxy(Class<T> type, CorfuRuntime rt, UUID streamID,
                                 Object[] args, ISerializer serializer)
        throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // Do we have a compiled proxy for this type?
        Class<ICorfuSMR<T>> proxyClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> proxyObject = null;
        if (args == null || args.length == 0) {
            proxyObject = proxyClass.newInstance();
        }
        else {
            // This loop is not ideal, but the easiest way to get around Java boxing,
            // which results in primitive constructors not matching.
            for (Constructor<?> constructor : proxyClass.getDeclaredConstructors()) {
                try {
                    proxyObject = (ICorfuSMR<T>) constructor.newInstance(args);
                    break;
                } catch (Exception e) {
                    // just keep trying until one works.
                }
            }
        }

        // Now we create the proxy, which actually manages
        // instances of this object.
        proxyObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, streamID,
                type, args, serializer,
                proxyObject.getCorfuSMRUpcallMap(),
                proxyObject.getCorfuUndoMap(),
                proxyObject.getCorfuUndoRecordMap()));

        if (proxyObject instanceof ICorfuSMRProxyContainer) {
            ((ICorfuSMRProxyContainer) proxyObject)
                    .setProxy$CORFUSMR(proxyObject.getCorfuSMRProxy());
        }

        return (T) proxyObject;
    }
}
