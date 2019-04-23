package org.corfudb.runtime.collections;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class is used to maintain the location information on different keys.
 * @param <T> the type of the map key.
 */
public class SMRLocationInfo<T>{
    // Keys have 1:1 mapping to locations.
    // keyToLocator maintains the locations corresponding to current key values.
    final private Map<T, ISMREntryLocator> keyToLocator = new HashMap<>();

    // It is possible for a SMREntry to affect multiple keys, for example "putAll".
    // keyCount maintains the number of active keys associated with one SMREntry.
    final private Map<ISMREntryLocator, Integer> keyCount = new HashMap<>();

    // It is possible for one global address to accommodate multiple SMREntries.
    // entryCount maintains the number of active SMREntries with one global address.
    final private Map<Long, Integer> entryCount = new HashMap<>();

    // lastLocator maintains the latest SMREntry synced to. lastLocator could only increase monotonically.
    // lastLocator is used to prevent duplicated location information.
    private ISMREntryLocator lastLocator = new SMREntry.SMREntryLocator(Address.NON_ADDRESS);

    public Optional<Long> addUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !canUpdate(newLocator)) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.put(key, newLocator);
        increaseKeyCount(newLocator);

        if (oldLocator != null) {
            return decreaseKeyCount(oldLocator);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Long> removeUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !canUpdate(newLocator)) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.remove(key);
        if (oldLocator != null) {
            return decreaseKeyCount(oldLocator);
        } else {
            return Optional.empty();
        }
    }

    public void clearUnsafe(@NonNull ISMREntryLocator newLocator) {
        if (canUpdate(newLocator)) {
            keyToLocator.clear();
            keyCount.clear();
            entryCount.clear();
        }
    }

    private boolean canUpdate(@NonNull ISMREntryLocator locator) {
        if (locator.compareTo(lastLocator) <= 0) {
            return false;
        } else {
            lastLocator = locator;
            return true;
        }
    }

    private void increaseKeyCount(ISMREntryLocator locator) {
        if (!keyCount.containsKey(locator)) {
            keyCount.put(locator, 0);
            increaseEntryCount(locator.getGlobalAddress());
        }

        keyCount.compute(locator, (l, c) -> c + 1);
    }

    private Optional<Long> decreaseKeyCount(ISMREntryLocator locator) {
        keyCount.computeIfPresent(locator, (l, c) -> c - 1);
        Integer count = keyCount.getOrDefault(locator, null);
        if (count != null && count.equals(0)) {
            keyCount.remove(locator);
            return decreaseEntryCount(locator.getGlobalAddress());
        }
        return Optional.empty();
    }

    private void increaseEntryCount(Long globalAddress) {
        entryCount.putIfAbsent(globalAddress, 0);
        entryCount.compute(globalAddress, (a, c) -> c + 1);
    }

    private Optional<Long> decreaseEntryCount(Long globalAddress) {
        entryCount.compute(globalAddress, (a, c) -> c - 1);
        if (entryCount.get(globalAddress) == 0) {
            entryCount.remove(globalAddress);
            return Optional.of(globalAddress);
        }

        return Optional.empty();
    }
}
