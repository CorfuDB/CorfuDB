package org.corfudb.runtime.collections;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.Address;

import java.util.Set;

public interface LocationBucket {
    LocationImpl EMPTY = new LocationImpl(Address.NON_ADDRESS, -1);

    interface Location {
        long getAddress();
        int getOffset();
    }

    @Data
    @AllArgsConstructor
    class LocationImpl implements Location {
        public LocationImpl() { }

        long address;
        int offset;
    }

    Set<LocationImpl> getLocation();
    void setLocation(long address, int offset);
}
