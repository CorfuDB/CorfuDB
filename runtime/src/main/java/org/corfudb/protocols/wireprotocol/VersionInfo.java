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
    Map<String, Object> optionsMap;

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
     * @param optionsMap    The options map used to start the server.
     * @param nodeId        The current node id.
     */
    public VersionInfo(Map<String,Object> optionsMap, @Nonnull String nodeId) {
        this.optionsMap = new HashMap<>(optionsMap);
        // Remove any non-serializable objects
        this.optionsMap.entrySet()
                .removeIf(e -> {
                    Object v = e.getValue();
                    return !(v instanceof Integer || v instanceof Long || v instanceof String);
                });
        this.nodeId = nodeId;
        this.version = CorfuRuntime.getVersionString();
    }
}
