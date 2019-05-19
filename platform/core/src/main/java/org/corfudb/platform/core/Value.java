package org.corfudb.platform.core;

/**
 * Type contract representing a data-containing entity.
 *
 * @author jameschang, jiaqic
 * @since 2017-06-06
 */
@FunctionalInterface
public interface Value {

    /**
     * Implementation of {@link Value} that represents an "empty" value.
     * <p>
     * This class should carry the same connotation as the return value of
     * {@linkplain java.util.Optional#empty() Optional&lt;Value&gt;.empty()}.
     */
    final class None implements Value {
        /**
         * Static singleton instance of {@link None}.
         */
        private static final None INSTANCE = new None();
        private static final String VALUE_STRING = "Value{None}";

        private byte[] data = new byte[0];

        private None() {
            //prevent creating instances
        }

        @Override
        public Type type() {
            return Type.NONE;
        }

        @Override
        public byte[] asBytes() {
            return data;
        }

        @Override
        public String toString() {
            return VALUE_STRING;
        }
    }

    /**
     * Enumeration of possible value types.
     * <p>
     * All types should have value identifier sized exactly 4-bytes as type signature.
     */
    enum Type {
        /**
         * Special type denoting that the associated {@link Value} instance is not a value.
         */
        NONE("NONE"),
        /**
         * Data is in unspecified encoding.
         */
        BINARY("BNRY"),
        /**
         * Data is encoded as UTF-8 JSON text.
         */
        JSON("JSON");

        private final String type;

        public String value() {
            return type;
        }

        Type(String type) {
            this.type = type;
        }

        public static Type from(final String type) {
            for (Type item : Type.values()) {
                if (type.equals(item.type)) {
                    return item;
                }
            }
            throw new IllegalArgumentException(type + " is not a valid type");
        }
    }

    /**
     * Data type.
     *
     * @return the type of data as {@link Type}.
     */
    default Type type() {
        return Value.Type.BINARY;
    }

    /**
     * Data as a native byte array.
     *
     * @return data in byte array.
     */
    byte[] asBytes();

    /**
     * Returns a {@link Value} instance that does not represent any value.
     *
     * @return an instance of {@link Value}.
     */
    static Value none() {
        return None.INSTANCE;
    }
}

