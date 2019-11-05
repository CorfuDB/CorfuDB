package org.corfudb.runtime.collections;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to maintain the location and other closely related information on different keys.
 * @param <T> the type of the map key.
 */
class MapLocatorStore<T> implements ILocatorStore<T> {
    // Keys have 1:1 mapping to SMRRecord
    private final Map<T, SMRRecordLocator> keyToSMRRecordLocator = new ConcurrentHashMap<>();

    // locator to host locator of last clear operation
    private SMRRecordLocator latestClearLocator = null;

    public static final List<SMRRecordLocator> EMPTY_LIST = new ArrayList<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SMRRecordLocator> addUnsafe(@NonNull T key, @NonNull SMRRecordLocator smrRecordLocator) {
        SMRRecordLocator oldSMRRecord = keyToSMRRecordLocator.put(key, smrRecordLocator);
        if (oldSMRRecord == null) {
            return EMPTY_LIST;
        }
        return Arrays.asList(oldSMRRecord);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SMRRecordLocator> clearUnsafe(@NonNull SMRRecordLocator clearLocator) {
        List<SMRRecordLocator> garbage = new ArrayList<>(keyToSMRRecordLocator.values());
        keyToSMRRecordLocator.clear();

        if (latestClearLocator != null) {
            garbage.add(latestClearLocator);
        }

        latestClearLocator = clearLocator;
        return garbage;
    }
}