package org.corfudb.universe.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@EqualsAndHashCode
public class IpAddress {
    @Getter
    private final String ip;

    @Override
    public String toString() {
        return ip;
    }
}
