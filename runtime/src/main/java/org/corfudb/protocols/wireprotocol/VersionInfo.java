package org.corfudb.protocols.wireprotocol;

import java.lang.management.ManagementFactory;
import java.util.Map;

import org.corfudb.runtime.CorfuRuntime;

import lombok.Getter;

/**
 * Created by mwei on 7/27/16.
 */
public class VersionInfo {
    @Getter
    Map<String, Object> optionsMap;

    @Getter
    long upTime = ManagementFactory.getRuntimeMXBean().getUptime();

    @Getter
    String startupArgs = System.getProperty("sun.java.command");

    @Getter
    String jvmUsed = System.getProperty("java.home") + "/bin/java";

    @Getter
    String version;

    public VersionInfo(Map<String,Object> optionsMap) {
        this.optionsMap = optionsMap;
        this.version = CorfuRuntime.getVersionString();
    }
}
