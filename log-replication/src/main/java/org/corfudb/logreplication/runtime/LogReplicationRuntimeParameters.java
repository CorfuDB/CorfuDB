package org.corfudb.logreplication.runtime;

import lombok.Data;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.corfudb.runtime.RuntimeParameters;

@Data
@SuperBuilder
public class LogReplicationRuntimeParameters extends RuntimeParameters {

    private String localCorfuEndpoint;

    private String remoteLogReplicationServerEndpoint;

}
