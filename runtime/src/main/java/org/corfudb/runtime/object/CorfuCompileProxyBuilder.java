package org.corfudb.runtime.object;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamView;

import java.util.Map;

/**
 * Created by mwei on 11/11/16.
 */
public class CorfuCompileProxyBuilder {

    @SuppressWarnings("unchecked")
    public static <T> T getProxy(Class<T> type, CorfuRuntime rt, StreamView sv)
        throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // Do we have a compiled proxy for this type?
        Class<ICorfuSMR<T>> proxyClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        // For now, we assume that we have a default constructor.
        ICorfuSMR<T> proxyObject = (proxyClass.newInstance());

        // Now we create the proxy, which actually manages
        // instances of this object.
        proxyObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, sv,
                type, proxyObject.getCorfuSMRUpcallMap()));

        return (T) proxyObject;
    }
}
