package org.corfudb.universe.node.server.vm;

import static org.corfudb.universe.node.server.CorfuServer.Mode;
import static org.corfudb.universe.node.server.CorfuServer.Persistence;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.StringJoiner;


/**
 * Represents the parameters for constructing a {@link VmCorfuServer}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class VmCorfuServerParams extends CorfuServerParams {
    private final VmName vmName;

    @Builder
    public VmCorfuServerParams(
            VmName vmName, int port, Mode mode, Persistence persistence,
            Level logLevel, String clusterName, Duration stopTimeout, String serverVersion,
            Path serverJarDirectory, String dockerImage) {

        super(
                port, mode, persistence, logLevel, clusterName, stopTimeout,
                Optional.empty(), serverVersion, serverJarDirectory, dockerImage
        );
        this.vmName = vmName;
    }

    @Builder
    @EqualsAndHashCode
    @Getter
    public static class VmName implements Comparable<VmName> {
        @NonNull
        private final String host;

        @Override
        public int compareTo(VmName other) {
            return host.compareTo(other.host);
        }

        @Override
        public String toString() {
            return host;
        }
    }
}
