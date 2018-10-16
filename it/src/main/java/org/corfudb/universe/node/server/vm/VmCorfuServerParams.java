package org.corfudb.universe.node.server.vm;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.slf4j.event.Level;

import java.time.Duration;

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
    public VmCorfuServerParams(String vmName, int port, Mode mode, Persistence persistence,
                               Level logLevel, String clusterName, Duration stopTimeout) {
        super(port, mode, persistence, logLevel, clusterName, stopTimeout);
        this.vmName = vmName;
    }
}
