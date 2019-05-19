package org.corfudb.platform.kv.core;


import org.corfudb.platform.core.Value;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Type contract representing an instance of associative-mapping.
 *
 * @see Key
 * @see Value
 * @see Version
 */
public final class Record<K extends Key, V extends Value, T extends Version> {

    private final K key;
    private final V value;
    private final T version;

    public Record(final K key, final V value, final T version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public T getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hashRes = 1;
        hashRes = prime * hashRes + ((key == null) ? 0 : key.hashCode());
        hashRes = prime * hashRes + ((value == null) ? 0 : Arrays.hashCode(value.asBytes()));
        hashRes = prime * hashRes + ((version == null) ? 0 : version.asString().hashCode());
        return hashRes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record that = (Record) o;

        return (key != null ?
                (that.getKey() != null && key.equals(that.getKey())) :
                that.getKey() == null) &&
                (value != null ?
                        (that.getValue() != null &&
                                value.type().value().equals(that.getValue().type().value()) &&
                                value.equals(that.getValue())) :
                        that.getValue() == null) &&
                (version != null ?
                        (that.getVersion() != null &&
                                version.asString().equals(that.getVersion().asString())) :
                        that.getVersion() == null);
    }

    @Override
    public String toString() {
        String strKey = (key != null ? key.toString() : null);
        String strValueType = (value != null ? value.type().value() : null);

        String strValue = null;
        if (strValueType != null && strValueType.equals(Value.Type.BINARY.value())) {
            strValue = "<binary data>";
        } else if (strValueType != null && strValueType.equals(Value.Type.JSON.value())) {
            strValue = new String(value.asBytes(), StandardCharsets.UTF_8);
        }

        String strVersion = (version != null ? version.asString() : null);

        return String.format("Record{key=%s, valueType=%s, value=%s, version=%s}",
                strKey, strValueType, strValue, strVersion);
    }
}
