package org.corfudb.runtime.collections;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;
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
    // The location of each key should be increase monotonically.
    final private Map<T, ISMREntryLocator> keyToLocator = new HashMap<>();

    // They could only increase monotonically.
    // TODO(Xin): It is more generic to replace the two variables with a locator.
    // TODO(Xin): Should the location be updated only after confirmation from garbage informer?
    private long lastMarkAddress = Address.NON_ADDRESS;
    private long lastMarkIndex = -1L;

    public Optional<ISMREntryLocator> addUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !afterLastMark(newLocator)) {
            return Optional.empty();
        }

        if (keyToLocator.containsKey(key) && keyToLocator.get(key).compareTo(newLocator) >= 0) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.put(key, newLocator);
        return Optional.ofNullable(oldLocator);
    }

    public Optional<ISMREntryLocator> removeUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !afterLastMark(newLocator)) {
            return Optional.empty();
        }

        if (keyToLocator.containsKey(key) && keyToLocator.get(key).compareTo(newLocator) >= 0) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.remove(key);
        return Optional.ofNullable(oldLocator);
    }

    public void clearUnsafe(@NonNull ISMREntryLocator newLocator) {
        if (afterLastMark(newLocator)) {
            keyToLocator.clear();
        }
    }

    private boolean afterLastMark(@NonNull ISMREntryLocator locator) {
        if (locator.getGlobalAddress() > lastMarkAddress  ||
                locator.getGlobalAddress() == lastMarkAddress && locator.getIndex() > lastMarkIndex) {
            lastMarkAddress = locator.getGlobalAddress();
            lastMarkIndex = locator.getIndex();
            return true;
        } else {
            return false;
        }
    }
}
