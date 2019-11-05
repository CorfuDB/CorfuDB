package org.corfudb.infrastructure.configuration;

import lombok.Getter;
import org.corfudb.infrastructure.ServerContext;


/**
 * Global configuration for Corfu.
 */
@Getter
public class CorfuConfig {
    private final ServerContext context;

    public CorfuConfig(ServerContext context) {
        this.context = context;
    }
}
