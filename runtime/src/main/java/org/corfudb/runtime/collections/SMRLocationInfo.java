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

    // lastLocator maintains the latest SMREntry synced to. lastLocator could only increase monotonically.
    // lastLocator is used to prevent duplicated location information.
    private ISMREntryLocator lastLocator = new SMREntry.SMREntryLocator(Address.NON_ADDRESS);

    public Optional<ISMREntryLocator> addUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !canUpdate(newLocator)) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.put(key, newLocator);
        return Optional.ofNullable(oldLocator);
    }

    public Optional<ISMREntryLocator> removeUnsafe(T key, @NonNull ISMREntryLocator newLocator) {
        if (key == null || !canUpdate(newLocator)) {
            return Optional.empty();
        }

        ISMREntryLocator oldLocator = keyToLocator.remove(key);
        return Optional.ofNullable(oldLocator);
    }

    public void clearUnsafe(@NonNull ISMREntryLocator newLocator) {
        if (canUpdate(newLocator)) {
            keyToLocator.clear();
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
}
