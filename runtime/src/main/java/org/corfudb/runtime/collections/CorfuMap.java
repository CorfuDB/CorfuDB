package org.corfudb.runtime.collections;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.corfudb.annotations.DontInstrument;

/** This class is a simple adapter around CorfuTable which
 *  presents a standard map-like interface.
 *
 *  <p> Keys must be unique, and for every key there is exactly one value
 *  mapped.
 *
 *  <p> Secondary index functions such as getByIndex are not available
 *  and will throw an UnsupportedOperationException.
 *
 * @param <K>   The type of the key (primary key) used in this map.
 * @param <V>   The type of the values (secondary key) used in this map.
 */
public class CorfuMap<K, V> extends CorfuTable<K, V, CorfuTable.NoSecondaryIndex, Void> {

    /** Getting by index is not supported in a CorfuMap, which only supports a single index.
     *
     * @param indexFunction The index function to use.
     * @param index         The index
     * @throws  UnsupportedOperationException   This operation is not supported on a CorfuMap.
     */
    @Override
    @DontInstrument
    public Collection<Object> getByIndex(@Nonnull NoSecondaryIndex indexFunction, Void index) {
        throw new UnsupportedOperationException("Can't get a CorfuMap by index!");
    }
}
