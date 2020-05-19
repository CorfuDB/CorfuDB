package org.corfudb.infrastructure;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.corfudb.runtime.RuntimeParameters;

@Data
@SuperBuilder
public class LogReplicationRuntimeParameters extends RuntimeParameters {

    private String localCorfuEndpoint;

    private String remoteLogReplicationServerEndpoint;

    private LogReplicationTransportType transport = LogReplicationTransportType.CUSTOM;

}
