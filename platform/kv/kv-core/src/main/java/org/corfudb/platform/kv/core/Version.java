package org.corfudb.platform.kv.core;

import org.corfudb.platform.core.Value;

/**
 * Type contract representing the temporal uniqueness attribute of a {@link Record}.
 */
public interface Version extends Value {
    /**
     * Opaque context containing data used by specific implementation of {@link Version}
     */
    interface Context {
    }

    /**
     * Denotes the ordering relationships between two {@link Version} instances.
     */
    enum Ordering {
        OLDER((byte) -1),
        SAME((byte) 0),
        NEWER((byte) 1),
        CONCURRENT(Byte.MAX_VALUE);

        private final byte value;

        Ordering(byte value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Returns an ordering which denotes happens-before relationship {@literal (->)}.
     *
     * <ul>
     * <li>this {@literal ->} other (i.e. this happens before other):
     * <pre>
     * {@code this.relativeTo(other)} == {@link Ordering#OLDER}
     * </pre>
     * </li>
     * <li>other {@literal ->} this (i.e. other happens before this):
     * <pre>
     * {@code this.relativeTo(other)} == {@link Ordering#NEWER}
     * </pre>
     * </li>
     * <li>this {@literal ==} other (i.e. equality is verifiable):
     * <pre>
     * {@code this.relativeTo(other)} == {@link Ordering#SAME}
     * </pre>
     * </li>
     * <li>!(this {@literal ->} other) {@literal &&} !(other {@literal ->} this)
     * {@literal &&} !(this {@literal ==} other)
     * (i.e. events appear to be concurrent):
     * <pre>
     * {@code this.relativeTo(other)} == {@link Ordering#CONCURRENT}
     * </pre>
     * </li>
     * </ul>
     *
     * @param other the other Version instance to compare to.
     * @return the {@link Ordering} relationship of this instance relative to the other instance.
     */
    Ordering relativeTo(Version other);

    /**
     * Returns an instance of the same type of {@link Version} such that
     * {@code this.increment(context).relativeTo(this)} returns {@link Ordering#NEWER}.
     *
     * @param context context that this Version instance is used in, in order to produce a meaningful
     *                incremental Version denoting a higher-ordered value.
     * @return an instance of {@link Version} denoting the next higher-order version value.
     */
    Version increment(Context context);

    /**
     * Returns a {@link Version} instance denoting an ordinal value that signals the initial value
     * in the range space of this {@link Version} instance.
     *
     * <p>
     * For all non-special-token Version instances {@code v}:
     * <pre>
     * {@code v.relativeTo(v.initial())} == {@link Ordering#NEWER}
     * </pre>
     *
     * @return an instance of {@link Version} denoting the lowest-order version value.
     */
    Version initial();

    /**
     * Returns a special token value denoting an ordinal value that signals the latest value
     * in the range space of this {@link Version} instance.
     *
     * <p>
     * For all non-special-token Version instances {@code v}:
     * <pre>
     * {@code v.relativeTo(v.latest())} == Ordering#OLDER}
     * </pre>
     *
     * @return an instance of {@link Version} denoting the highest-order version value.
     */
    Version latest();

    /**
     * Returns a normalized form of the {@link Version} instance for data exchange tasks, such as
     * serialization.
     *
     * @return a normalized form of this instance as a {@link String}.
     */
    String asString();

    /**
     * Type identifier uniquely identifying the implementation of this {@link Version} instance.
     *
     * @return type identifier as a {@link String}.
     */
    String versionType();
}