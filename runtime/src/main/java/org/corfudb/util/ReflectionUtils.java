package org.corfudb.util;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 3/29/16.
 */
@Slf4j
public class ReflectionUtils {
    static Map<String, Class> primitiveTypeMap = ImmutableMap.<String, Class>builder()
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
    private static Pattern methodExtractor = Pattern.compile("([^.\\s]*)\\((.*)\\)$");
    private static Pattern classExtractor = Pattern.compile("(\\S*)\\.(.*)\\(.*$");

    public static String getShortMethodName(String longName) {
        int packageIndex = longName.substring(0, longName.indexOf("(")).lastIndexOf(".");
        return longName.substring(packageIndex + 1);
    }

    public static String getMethodNameOnlyFromString(String s) {
        return s.substring(0, s.indexOf("("));
    }

    public static Class<?> getPrimitiveType(String s) {
        return primitiveTypeMap.get(s);
    }

    /**
     * Get arguments from method signature.
     * @param s String representation of signature
     * @return Array of types
     */
    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.contains("(") ? s.substring(s.indexOf("(") + 1, s.length() - 1) : s;
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
     * Extract argument types from a string that reperesents the method
     * signature.
     * @param args Signature string
     * @return Array of types
     */
    public static Class[] getArgumentTypesFromArgumentList(Object[] args) {
        return Arrays.stream(args)
                .map(Object::getClass)
                .toArray(Class[]::new);
    }

    public static <T> T newInstanceFromUnknownArgumentTypes(Class<T> cls, Object[] args) {
        try {
            return cls.getDeclaredConstructor(getArgumentTypesFromArgumentList(args))
                    .newInstance(args);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException ie) {
            throw new RuntimeException(ie);
        } catch (NoSuchMethodException nsme) {
            // Now we need to find a matching primitive type.
            // We can maybe cheat by running through all the constructors until we get what we want
            // due to autoboxing
            Constructor[] ctors = Arrays.stream(cls.getDeclaredConstructors())
                    .filter(c -> c.getParameterTypes().length == args.length)
                    .toArray(Constructor[]::new);
            for (Constructor<?> ctor : ctors) {
                try {
                    return (T) ctor.newInstance(args);
                } catch (Exception e) {
                    // silently drop exceptions since we are brute forcing here...
                }
            }

            String argTypes = Arrays.stream(args)
                    .map(Object::getClass)
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));

            String availableCtors = Arrays.stream(ctors)
                    .map(Constructor::toString)
                    .collect(Collectors.joining(", "));

            throw new RuntimeException("Couldn't find a matching ctor for "
                    + argTypes + "; available ctors are " + availableCtors);
        }
    }

    /**
     * Extract method name from to string path.
     * @param methodString Method signature in the form of a string
     * @return Method object
     */
    public static Method getMethodFromToString(String methodString) {
        Class<?> cls = getClassFromMethodToString(methodString);
        Matcher m = methodExtractor.matcher(methodString);
        m.find();
        try {
            return cls.getDeclaredMethod(m.group(1), getArgumentTypesFromString(m.group(2)));
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException(nsme);
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
