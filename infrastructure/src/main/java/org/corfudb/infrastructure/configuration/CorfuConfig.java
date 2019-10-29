package org.corfudb.infrastructure.configuration;

import lombok.Getter;
import org.corfudb.infrastructure.ServerContext;

import java.util.Map;

/**
 * Global configuration for Corfu.
 */
@Getter
public class CorfuConfig {
    private final Map<String, Object> config;
    private final ServerContext context;

    public CorfuConfig(ServerContext context) {
        this.context = context;
        this.config = context.getServerConfig();
    }
}
