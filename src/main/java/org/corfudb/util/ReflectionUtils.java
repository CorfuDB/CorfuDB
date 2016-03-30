package org.corfudb.util;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by mwei on 3/29/16.
 */
@Slf4j
public class ReflectionUtils {
    public static String getShortMethodName(String longName)
    {
        int packageIndex = longName.substring(0, longName.indexOf("(")).lastIndexOf(".");
        return longName.substring(packageIndex + 1);
    }

    public static String getMethodNameOnlyFromString(String s) {
        return s.substring(0, s.indexOf("("));
    }

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

    public static Class<?> getPrimitiveType(String s) {
        return primitiveTypeMap.get(s);
    }

    public static Class[] getArgumentTypesFromString(String s) {
        String argList = s.substring(s.indexOf("(") + 1, s.length() - 1);
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
}
