package org.corfudb.universe.node.server.vm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.corfudb.universe.node.server.CorfuServer.Mode;
import static org.corfudb.universe.node.server.CorfuServer.Persistence;


/**
 * Represents the parameters for constructing a {@link VmCorfuServer}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class VmCorfuServerParams extends CorfuServerParams {
    private final String vmName;

    @Builder
    public VmCorfuServerParams(
            String vmName, int port, Mode mode, Persistence persistence,
            Level logLevel, String clusterName, Duration stopTimeout, String serverVersion,
            Path serverJarDirectory, String dockerImage) {

        super(
                port, mode, persistence, logLevel, clusterName, stopTimeout,
                Optional.empty(), serverVersion, serverJarDirectory, dockerImage
        );
        this.vmName = vmName;
    }
}
