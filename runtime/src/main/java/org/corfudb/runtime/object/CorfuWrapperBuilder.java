package org.corfudb.runtime.object;

import java.lang.reflect.Constructor;

import javax.annotation.Nonnull;

import org.corfudb.runtime.view.ObjectBuilder;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuWrapperBuilder {

    static <T> VersionedObjectManager<T> generateManager(@Nonnull ICorfuWrapper<T> w,
                                                         @Nonnull ObjectBuilder<T> builder) {
        return new VersionedObjectManager<>(new LinearizableStateMachineStream(
                        builder.getRuntime(),
                        builder.getRuntime()
                                .getStreamsView().get(builder.getStreamId()),
                        builder.getSerializer()),
                w, builder);
    }

    /**
     * Returns a wrapper for the underlying SMR Object
     *
     * @param <T>        Type
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getWrapper(ObjectBuilder<T> builder)
            throws ClassNotFoundException, IllegalAccessException,
            InstantiationException {
        // Do we have a compiled wrapper for this type?
        Class<ICorfuWrapper<T>> wrapperClass = (Class<ICorfuWrapper<T>>)
                Class.forName(builder.getType().getName() + ICorfuWrapper.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuWrapper<T> wrapperObject = null;
        if ((builder.getArguments() == null || builder.getArguments().length == 0)) {
            try {
                wrapperObject = wrapperClass.getDeclaredConstructor(IManagerGenerator.class)
                        .newInstance((IManagerGenerator<T>) w -> generateManager(w, builder));
            } catch (Exception e) {
                throw new RuntimeException("Failed to build object", e);
            }
        } else {
            // This loop is not ideal, but the easiest way to get around Java
            // boxing, which results in primitive constructors not matching.
            for (Constructor<?> constructor : wrapperClass
                    .getDeclaredConstructors()) {
                try {
                    Object[] arguments = new Object[builder.getArguments().length + 1];
                    IManagerGenerator<T> generator = w -> generateManager(w, builder);
                    arguments[0] = generator;
                    for (int i = 0; i < builder.getArguments().length; i++) {
                        arguments[i + 1] = builder.getArguments()[i];
                    }
                    wrapperObject = (ICorfuWrapper<T>) constructor.newInstance(arguments);
                    break;
                } catch (Exception e) {
                    // just keep trying until one works.
                }
            }
        }

        if (wrapperObject == null) {
            throw new RuntimeException("Failed to generate object, all target constructors"
                    + " exhausted");
        }

        return (T) wrapperObject;
    }
}
