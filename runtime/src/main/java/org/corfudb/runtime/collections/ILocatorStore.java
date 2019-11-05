package org.corfudb.runtime.collections;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;

import java.util.List;

/**
 * This is the interface for the object to store SRMRecord location information.
 * @param <T> Type of the object required to identify garbage when manipulating this store.
 */
public interface ILocatorStore<T> {
    /**
     * Add a new SMRRecordLocator instance to this store.
     * @param obj object necessary for garbage identification.
     * @param smrRecordLocator locator.
     * @return A list of SMRRecordLocators that point to garbage.
     */
    List<SMRRecordLocator> addUnsafe(@NonNull T obj, @NonNull SMRRecordLocator smrRecordLocator);

    /**
     * Clear this store.
     * @param latestClearLocator
     *
     * @return stored locators.
     */
    List<SMRRecordLocator> clearUnsafe(@NonNull SMRRecordLocator latestClearLocator);
}
