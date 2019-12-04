package org.corfudb.protocols.wireprotocol;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by mwei on 7/27/16.
 */
public class VersionInfo {
    @Getter
    long upTime = ManagementFactory.getRuntimeMXBean().getUptime();

    @Getter
    String startupArgs = System.getProperty("sun.java.command");

    @Getter
    String jvmUsed = System.getProperty("java.home") + "/bin/java";

    @Getter
    String version;

    @Getter
    String nodeId;

    /** Create a new version info, using the current options map and node id.
     *
     * @param nodeId        The current node id.
     */
    public VersionInfo(@Nonnull String nodeId) {
        this.nodeId = nodeId;
        this.version = CorfuRuntime.getVersionString();
    }
}
