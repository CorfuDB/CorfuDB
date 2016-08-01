package org.corfudb.protocols.wireprotocol;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created by mwei on 7/27/16.
 */
public class VersionInfo {
    @Getter
    Map<String, Object> optionsMap;

    @Getter
    long upTime = ManagementFactory.getRuntimeMXBean().getUptime();

    public VersionInfo(Map<String,Object> optionsMap) {
        this.optionsMap = optionsMap;
    }
}
