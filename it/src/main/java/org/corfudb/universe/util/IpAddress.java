package org.corfudb.universe.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Builder
@EqualsAndHashCode
public class IpAddress {

    @Getter
    @NonNull
    private final String ip;

    @Override
    public String toString() {
        return ip;
    }
}
