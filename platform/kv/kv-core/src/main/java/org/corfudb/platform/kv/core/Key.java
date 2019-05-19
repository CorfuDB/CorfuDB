package org.corfudb.platform.kv.core;

import org.corfudb.platform.core.ByteString;
import org.corfudb.platform.core.Value;

/**
 * Type contract representing of the key of a key-value associative mapping.
 *
 */
@FunctionalInterface
public interface Key extends Comparable<Key>, Value {

    @Override
    @SuppressWarnings("FunctionalInterfaceMethodChanged")
    default byte[] asBytes() {
        return get().asBytes();
    }

    @Override
    default int compareTo(Key other) {
        int comparison = get().compareTo(other.get());
        if (comparison == 0) {
            // Use Enum's natural ordering as tie-breaker.
            return type().compareTo(other.type());
        } else {
            return comparison;
        }
    }

    /**
     * Obtain the content of the key.
     * <p>
     * Note: {@link Object#equals}, {@link Object#hashCode}, and {@link #compareTo} should be
     * implemented in a consistent way that utilizes the return value of this method.
     *
     * @return    an instance of {@link ByteString} representing the key content in binary.
     */
    ByteString get();
}
