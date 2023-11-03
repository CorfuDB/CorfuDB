package org.corfudb.infrastructure.utils;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutRateLimitParams;

import static org.corfudb.common.config.ConfigParamNames.LAYOUT_RATE_LIMIT_COOLDOWN_MULTIPLIER;
import static org.corfudb.common.config.ConfigParamNames.LAYOUT_RATE_LIMIT_RESET_MULTIPLIER;
import static org.corfudb.common.config.ConfigParamNames.LAYOUT_RATE_LIMIT_TIMEOUT;

/**
 * Created by cgudisagar on 11/2/23.
 */
public final class ServerContextUtils {
    public static LayoutRateLimitParams buildRateLimitParamsFromServerContext(ServerContext serverContext) {

        int layoutRateTimeout =
                serverContext
                        .<String>getServerConfig(LAYOUT_RATE_LIMIT_TIMEOUT)
                        .map(Integer::parseInt)
                        .orElse(LayoutRateLimitParams.DEFAULT_TIMEOUT);

        double layoutRateLimitResetMultiplier =
                serverContext
                        .<String>getServerConfig(LAYOUT_RATE_LIMIT_RESET_MULTIPLIER)
                        .map(Double::parseDouble)
                        .orElse(LayoutRateLimitParams.DEFAULT_LAYOUT_RATE_LIMIT_RESET_MULTIPLIER);

        double layoutRateLimitCooldownMultiplier =
                serverContext
                        .<String>getServerConfig(LAYOUT_RATE_LIMIT_COOLDOWN_MULTIPLIER)
                        .map(Double::parseDouble)
                        .orElse(LayoutRateLimitParams.DEFAULT_LAYOUT_RATE_LIMIT_COOLDOWN_MULTIPLIER);

        return LayoutRateLimitParams.builder()
                .timeout(layoutRateTimeout)
                .resetMultiplier(layoutRateLimitResetMultiplier)
                .cooldownMultiplier(layoutRateLimitCooldownMultiplier).build();
    }

    private ServerContextUtils() {
        //prevent creating instances
    }
}
