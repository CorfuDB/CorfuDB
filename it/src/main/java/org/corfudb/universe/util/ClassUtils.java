package org.corfudb.universe.util;

import java.util.Objects;

public class ClassUtils {

    private ClassUtils() {
        //prevent creating class util instances
    }

    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj) {
        Objects.requireNonNull(obj);
        return (T) obj;
    }

    public static <T> T cast(Object obj, Class<T> objType) {
        Objects.requireNonNull(obj);
        return objType.cast(obj);
    }
}
