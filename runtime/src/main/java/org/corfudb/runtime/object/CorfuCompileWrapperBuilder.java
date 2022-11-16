package org.corfudb.runtime.object;

import java.util.Set;
import java.util.UUID;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {

    static final String PERSISTENT_CORFU_TABLE_CLASS_NAME = "org.corfudb.runtime.collections.PersistentCorfuTable";
    static final String IMMUTABLE_CORFU_TABLE_CLASS_NAME = "org.corfudb.runtime.collections.ImmutableCorfuTable";

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
    private static <T extends ICorfuSMR<T>> T getWrapper(Class<T> type, CorfuRuntime rt,
                                                         UUID streamID, Object[] args,
                                                         ISerializer serializer,
                                                         Set<UUID> streamTags,
                                                         ObjectOpenOption objectOpenOption) throws Exception {

        if (type.getName().equals(PERSISTENT_CORFU_TABLE_CLASS_NAME)) {
            // TODO: make general - This should get cleaned up
            Class<T> immutableClass = (Class<T>)
                    Class.forName(IMMUTABLE_CORFU_TABLE_CLASS_NAME);

            Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>) Class.forName(type.getName());

            // Instantiate a new instance of this class.
            ICorfuSMR<T> wrapperObject = (ICorfuSMR<T>) ReflectionUtils.
                    findMatchingConstructor(wrapperClass.getDeclaredConstructors(), new Object[0]);

            // Note: args are used when invoking the internal immutable data structure constructor
            wrapperObject.setProxy$CORFUSMR(new MVOCorfuCompileProxy<>(rt, streamID,
                    immutableClass, args, serializer, streamTags, wrapperObject, objectOpenOption));
            return (T) wrapperObject;
        }

        // Do we have a compiled wrapper for this type?
        Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> wrapperObject = (ICorfuSMR<T>) ReflectionUtils.
                findMatchingConstructor(wrapperClass.getDeclaredConstructors(), args);

        // Now we create the proxy, which actually manages
        // instances of this object. The wrapper delegates calls to the proxy.
        wrapperObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, streamID,
                type, args, serializer, streamTags, wrapperObject, objectOpenOption));

        return (T) wrapperObject;
    }

    public static <T extends ICorfuSMR<T>> T getWrapper(SMRObject<T> smrObject) throws Exception {
        return getWrapper(smrObject.getType(),
                smrObject.getRuntime(),
                smrObject.getStreamID(),
                smrObject.getArguments(),
                smrObject.getSerializer(),
                smrObject.getStreamTags(),
                smrObject.getOption());
    }
}
