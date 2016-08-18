package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.ObjectExistsException;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

/**
 * Created by mwei on 3/30/16.
 */
@Slf4j
public class CorfuProxyBuilder {

    final static AnnotationDescription instrumentedDescription =
            AnnotationDescription.Builder.ofType(Instrumented.class)
                    .build();

    final static AnnotationDescription instrumentedObjectDescription =
            AnnotationDescription.Builder.ofType(InstrumentedCorfuObject.class)
                    .build();

    public static <T> DynamicType.Builder<T> instrumentSMRMethods(CorfuObjectProxy proxy,
                                                                  DynamicType.Builder<T> bb) {
        try {
            bb = bb.method(ElementMatchers.isAnnotatedWith(Mutator.class)
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                    .intercept(MethodDelegation.to(proxy, "mutator").filter(ElementMatchers.named("interceptMutator")))
                    .annotateMethod(instrumentedDescription);
        } catch (NoSuchMethodError nsme) {
            log.trace("Class {} has no mutators", proxy.getOriginalClass());
        }
        try {
            bb = bb.method(ElementMatchers.isAnnotatedWith(Accessor.class)
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                    .intercept(MethodDelegation.to(proxy, "accessor").filter(ElementMatchers.named("interceptAccessor")))
                    .annotateMethod(instrumentedDescription);
        } catch (NoSuchMethodError nsme) {
            log.trace("Class {} has no accessors", proxy.getOriginalClass());
        }
        try {
            bb = bb.method(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class))))
                    .intercept(MethodDelegation.to(proxy, "maccessor").filter(ElementMatchers.named("interceptMutatorAccessor")))
                    .annotateMethod(instrumentedDescription);
        } catch (NoSuchMethodError nsme) {
            log.trace("Class {} has no mutatoraccessors", proxy.getOriginalClass());
        }


        bb = bb.implement(ICorfuSMRObject.class)
                .method(ElementMatchers.named("getSMRObject"))
                .intercept(MethodDelegation.to(proxy, "getSMRObject").filter(ElementMatchers.named("interceptGetSMRObject")))
                .method(ElementMatchers.named("registerPostHandler"))
                .intercept(MethodDelegation.to(proxy, "registerPostHandler").filter(ElementMatchers.named("registerPostHandler")));
        return bb;
    }

    public static <T> DynamicType.Builder<T> instrumentCorfuObjectMethods(CorfuObjectProxy proxy,
                                                                          DynamicType.Builder<T> bb) {
        return bb.implement(ICorfuObject.class)
                .method(ElementMatchers.named("getStreamID"))
                .intercept(MethodDelegation.to(proxy, "getStreamID").filter(ElementMatchers.named("getStreamID")))
                .method(ElementMatchers.named("getProxy"))
                .intercept(MethodDelegation.to(proxy, "getProxy").filter(ElementMatchers.named("getProxy")))
                .method(ElementMatchers.named("getRuntime"))
                .intercept(MethodDelegation.to(proxy, "getRuntime").filter(ElementMatchers.named("getRuntime")))
                .method(ElementMatchers.isAnnotatedWith(TransactionalMethod.class))
                .intercept(MethodDelegation.to(proxy, "handleTX").filter(ElementMatchers.named("handleTransactionalMethod")));
    }

    @SuppressWarnings("unchecked")
    public static <T, R extends ISMRInterface> Class<? extends T>
    getProxyClass(CorfuObjectProxy proxy, Class<T> type, Class<R> overlay) {
        if (type.isAnnotationPresent(CorfuObject.class)) {
            log.trace("Detected CorfuObject({}), instrumenting methods.", type);
            CorfuObject corfuAnnotation = type.getAnnotation(CorfuObject.class);
            DynamicType.Builder<T> b = new ByteBuddy()
                    .subclass(type);

            b = instrumentCorfuObjectMethods(proxy, b);
            if (corfuAnnotation.objectType() == ObjectType.SMR) {
                // Make all methods public, so we can instrument them.
                Method[] ms = type.getDeclaredMethods();
                for (Method m : ms) {
                    if (m.getModifiers() == 0) {
                        log.warn("Method {} is not public, please mark it as public if you wish to call it via SMR");
                    }
                }

                b = instrumentSMRMethods(proxy, b);

                // default methods to MutatorAccessors
                b = b.method(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Mutator.class))
                        .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Accessor.class)))
                        .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)))
                        .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(DontInstrument.class)))
                        .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class)))
                        .and(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class)))
                        .and(ElementMatchers.not(ElementMatchers.isDefaultMethod())))
                        .intercept(MethodDelegation.to(proxy, "mutatorAccessor").filter(ElementMatchers.named("interceptMutatorAccessor")))
                        .annotateMethod(instrumentedDescription);
            }

            Class<? extends T> generatedClass = b.make()
                    .load(CorfuSMRObjectProxy.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            proxy.setGeneratedClass(generatedClass);
            return generatedClass;
        }


        /** TODO: Legacy code which may need cleanup. */
        else if (Arrays.stream(type.getInterfaces()).anyMatch(ICorfuSMRObject.class::isAssignableFrom)) {
            log.trace("Detected ICorfuSMRObject({}), instrumenting methods.", type);

            DynamicType.Builder<T> b = new ByteBuddy()
                    .subclass(type);

            b = instrumentCorfuObjectMethods(proxy, b);
            b = instrumentSMRMethods(proxy, b);


            Class<? extends T> generatedClass = b.make()
                    .load(CorfuSMRObjectProxy.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();
            proxy.generatedClass = generatedClass;
            return generatedClass;
        } else if (overlay != null) {
            log.trace("Detected Overlay({}), instrumenting methods", overlay);
        } else if (Arrays.stream(type.getInterfaces()).anyMatch(ISMRInterface.class::isAssignableFrom)) {
            ISMRInterface[] iface = Arrays.stream(type.getInterfaces())
                    .filter(ISMRInterface.class::isAssignableFrom)
                    .toArray(ISMRInterface[]::new);
            log.trace("Detected ISMRInterfaces({}), instrumenting methods", iface);
        } else {
            log.trace("{} is not an ICorfuSMRObject, no ISMRInterfaces and no overlay provided. " +
                    "Instrumenting all methods as mutatorAccessors but respecting annotations", type);

            // dump all method annotations
            log.trace("All methods for {}:", type);
            if (log.isTraceEnabled()) {
                Method[] ms = type.getMethods();
                for (Method m : ms) {
                    log.trace("{}: {}", m.getName(), m.getAnnotations());
                }
            }
            DynamicType.Builder<T> bb = new ByteBuddy().subclass(type);

            bb = instrumentCorfuObjectMethods(proxy, bb);
            bb = instrumentSMRMethods(proxy, bb);
            bb = bb.method(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Mutator.class))
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Accessor.class)))
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(MutatorAccessor.class)))
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(DontInstrument.class)))
                    .and(ElementMatchers.not(ElementMatchers.isAnnotatedWith(Instrumented.class)))
                    .and(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class)))
                    .and(ElementMatchers.not(ElementMatchers.isDefaultMethod())))
                    .intercept(MethodDelegation.to(proxy, "mutatorAccessor").filter(ElementMatchers.named("interceptMutatorAccessor")))
                    .annotateMethod(instrumentedDescription);

            bb.annotateType(instrumentedObjectDescription);
            Class<? extends T> generatedClass =
                    bb.make().load(CorfuSMRObjectProxy.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                            .getLoaded();
            proxy.setGeneratedClass(generatedClass.getClass());
            return generatedClass;
        }
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public static <T, R extends ISMRInterface>
    T getProxy(@NonNull Class<T> type, Class<R> overlay, @NonNull StreamView sv, @NonNull CorfuRuntime runtime,
               Serializers.SerializerType serializer, Set<ObjectOpenOptions> options, Object... constructorArgs) {
        try {
            CorfuObjectProxy<T> proxy;

            if (type.isAnnotationPresent(CorfuObject.class)) {
                CorfuObject annotation = type.getAnnotation(CorfuObject.class);
                if (annotation.objectType() == ObjectType.SMR) {
                    proxy = new CorfuSMRObjectProxy<>(runtime, sv, type, serializer);
                    if (annotation.stateSource().equals(StateSource.SELF)) {
                        ((CorfuSMRObjectProxy) proxy).setSelfState(true);
                    } else {
                        ((CorfuSMRObjectProxy) proxy).setStateClass(annotation.stateType());
                    }
                } else {
                    proxy = new CorfuObjectProxy<>(runtime, sv, type, serializer);
                }

                if (annotation.constructorType() == ConstructorType.PERSISTED) {
                    // we need to either persist the constructor, or load from the saved args...
                    long token = sv.check();
                    boolean readConstructor = true;
                    if (token == -1L) {
                        log.debug("There appears to be no constructor for {}, writing one.", sv.getStreamID());
                        // "default" is the SMRMethod name we use because it is also a Java reserved keyword.
                        long streamStart = sv.acquireAndWrite(new SMREntry("default", constructorArgs, serializer),
                                t -> t.getBackpointerMap().get(sv.getStreamID()) == -1L,
                                t -> false);
                        readConstructor = streamStart == -1L;
                        if (annotation.objectType() == ObjectType.SMR) {
                            ((CorfuSMRObjectProxy) proxy).setCreationArguments(constructorArgs);
                        }
                    }
                    if (readConstructor) {
                        if (options.contains(ObjectOpenOptions.CREATE_ONLY)) {
                            throw new ObjectExistsException(token);
                        }
                        log.debug("There appears to be an existing constructor for {}, reading it...", sv.getStreamID());
                        // The "default" entry should be the first entry in the stream.
                        // TODO: handle garbage collected streams.
                        LogData entry = sv.read();
                        while (entry != null) {
                            if (entry.getPayload(runtime) instanceof SMREntry &&
                                    ((SMREntry) entry.getPayload(runtime)).getSMRMethod().equals("default")) {
                                log.trace("Setting contructor arguments to {}", ((SMREntry) entry.getPayload(runtime))
                                        .getSMRArguments());
                                constructorArgs = ((SMREntry) entry.getPayload(runtime)).getSMRArguments();
                                if (annotation.objectType() == ObjectType.SMR) {
                                    ((CorfuSMRObjectProxy) proxy).setCreationArguments(constructorArgs);
                                }
                                break;
                            }
                            entry = sv.read();
                        }
                    }
                }
            } else {
                proxy = new CorfuSMRObjectProxy<>(runtime, sv, type, serializer);
            }
            T ret;
            if (constructorArgs == null || constructorArgs.length == 0) {
                ret = getProxyClass(proxy, type, overlay).newInstance();
            } else {
                ret = ReflectionUtils.newInstanceFromUnknownArgumentTypes(getProxyClass(proxy, type, overlay),
                        constructorArgs);
            }
            if (proxy instanceof CorfuSMRObjectProxy) {
                ((CorfuSMRObjectProxy) proxy).calculateMethodHashTable(ret.getClass());
            }
            if (type.isAnnotationPresent(CorfuObject.class)) {
                CorfuObject annotation = type.getAnnotation(CorfuObject.class);
                if (annotation.objectType().equals(ObjectType.SMR) &&
                        annotation.stateSource().equals(StateSource.SELF)) {
                    ((CorfuSMRObjectProxy) proxy).setUnderlyingObject(ret);
                }
            }
            return ret;
        } catch (InstantiationException | IllegalAccessException ie) {
            throw new RuntimeException("Unexpected exception opening object", ie);
        }
    }
}
