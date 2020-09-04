package org.corfudb.common.util;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A generic class that holds two elements.
 * @param <T> First element of a tuple.
 * @param <U> Second element of a tuple.
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Tuple<T, U> {
    public final T first;
    public final U second;

}
