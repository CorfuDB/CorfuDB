package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mwei on 3/29/16.
 */
@Slf4j
public class ReflectionUtils {

    private ReflectionUtils() {
        // prevent instantiation of this class
    }

    static final Map<String, Class> primitiveTypeMap = ImmutableMap.<String, Class>builder()
            .put("int", Integer.TYPE)
            .put("long", Long.TYPE)
            .put("double", Double.TYPE)
            .put("float", Float.TYPE)
            .put("bool", Boolean.TYPE)
            .put("char", Character.TYPE)
            .put("byte", Byte.TYPE)
            .put("void", Void.TYPE)
            .put("short", Short.TYPE)
            .put("int[]", int[].class)
            .put("long[]", long[].class)
            .put("double[]", double[].class)
            .put("float[]", float[].class)
            .put("bool[]", boolean[].class)
            .put("char[]", char[].class)
            .put("byte[]", byte[].class)
            .put("short[]", short[].class)
            .build();
    private static final Pattern classExtractor = Pattern.compile("(\\S*)\\.(.*)\\(.*$");

    public static Class<?> getPrimitiveType(String s) {
        return primitiveTypeMap.get(s);
    }

    /**
     * Get arguments from method signature.
     * @param s String representation of signature
     * @return Array of types
     */
    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.contains("(") ? s.substring(s.indexOf('(') + 1, s.length() - 1) : s;
        return Arrays.stream(argList.split(","))
                .filter(x -> !x.equals(""))
                .map(x -> {
                    try {
                        return Class.forName(x);
                    } catch (ClassNotFoundException cnfe) {
                        Class retVal = getPrimitiveType(x);
                        if (retVal == null) {
                            log.warn("Class {} not found", x);
                        }
                        return retVal;
                    }
                })
                .toArray(Class[]::new);
    }

    /**
     * Given the constructor arguments and the desired class type, find
     * the closest matching constructor. For example, if a wrapper
     * class contains two constructors, Constructor(A) and Constructor(B),
     * and the constructor argument is of type B, where B inherits from A,
     * instantiate the object via Constructor(B) since it is closes with
     * respect to type hierarchy.
     *
     * @param constructors   Object constructors.
     * @param args           Constructor arguments.
     * @param <T>            Type
     * @return instantiated  wrapper class.
     */
    public static <T> T findMatchingConstructor(Constructor<?>[] constructors, Object[] args) {

        // Figure out the argument types.
        final List<Class<?>> argTypes = Arrays.stream(args)
                .map(Object::getClass)
                .collect(Collectors.toList());

        // Filter out the constructors that do not have the same arity.
        final List<Constructor<?>> constructorCandidates = Arrays.stream(constructors)
                .filter(constructor -> constructor.getParameterTypes().length == args.length)
                .collect(Collectors.toList());

        Map<Integer, Constructor<?>> matchingConstructors = new HashMap<>();
        for (Constructor<?> candidate: constructorCandidates) {
            final List<Class<?>> parameterTypes = Arrays.asList(candidate.getParameterTypes());
            final List<Integer> resolutionList = new LinkedList<>();
            // Compare each argument type with the corresponding constructor parameter type.
            for (int idx = 0; idx < parameterTypes.size(); idx++) {
                final Class<?> parameterType = parameterTypes.get(idx);
                final Class<?> argumentType = argTypes.get(idx);
                // If we are able to match the argument type with the constructor parameter
                // increment the hierarchy depth, signaling the distance between the
                // argument type and the parameter type in terms of type hierarchy.
                resolutionList.add(maxDepth(parameterType, argumentType, 0));
            }

            // If any of the argument types has type distance of zero, this means
            // that we are unable to match the current constructor with the given arguments.
            if (resolutionList.stream().anyMatch(depth -> depth == 0)) {
                continue;
            }

            // Put all matching constructors in a map in form of:
            // (L1 Norm (Distance) -> Constructor)
            Integer key = resolutionList.stream().reduce(0, Integer::sum);
            matchingConstructors.put(key, candidate);
        }

        // Instantiate the wrapper object with a constructor that has the lowest L1 norm.
        Object instance;
        try {
            instance = matchingConstructors
                    .entrySet()
                    .stream()
                    .min(Map.Entry.comparingByKey())
                    .orElseThrow(() -> new IllegalStateException("No matching constructors found."))
                    .getValue()
                    .newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(e);
        }

        return org.corfudb.common.util.ClassUtils.cast(instance);
    }

    /**
     * Return the distance between two types with respect to their inheritance.
     *
     * @param parameterType type associated with the constructor definition
     * @param argumentType type associated with the provided argument
     * @param currentDepth recursive parameter
     * @return 0 if if one Class cannot be assigned to a variable of another Class.
     *         Positive integer otherwise.
     */
    private static int maxDepth(Class parameterType, Class argumentType, int currentDepth) {
        if (!ClassUtils.isAssignable(argumentType, parameterType, true)) {
            return currentDepth;
        } else {
            final int newDepth = currentDepth + 1;
            final List<Integer> results = new ArrayList<>();
            final Class[] interfaces = argumentType.getInterfaces();
            final Class superClass = argumentType.getSuperclass();

            Arrays.stream(interfaces) // Check the interface hierarchy recursively.
                    .forEach(clazz -> results.add(maxDepth(parameterType, clazz, newDepth)));
            Optional.ofNullable(superClass) // Check the concrete hierarchy recursively.
                    .map(clazz -> results.add(maxDepth(parameterType, clazz, newDepth)));
            return results.stream()
                    .max(Comparator.comparing(Integer::valueOf))
                    .orElse(newDepth);
        }
    }

    /**
     * Extract class name from method to string path.
     * @param methodString toString method path
     * @return String representation of the class name
     */
    public static Class getClassFromMethodToString(String methodString) {
        Matcher m = classExtractor.matcher(methodString);
        m.find();
        String className = m.group(1);
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
    }
}
