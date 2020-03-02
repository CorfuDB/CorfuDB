package org.corfudb.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.*;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Utility to Reboot a server which includes reset or restart
 * Reset wipes out all existing Corfu data while restart does not.
 *
 * <p>Created by WenbinZhu on 4/18/19.
 */
@Slf4j
public class RebootUtil {

    private RebootUtil() {
        // prevent instantiation of this class
    }

    /**
     * A default cluster Id for resets if none are provided by a user.
     */
    private static final UUID DEFAULT_CLUSTER_ID = Layout.INVALID_CLUSTER_ID;
    /**
     * Resets the given server.
     * Attempts to reset a server finite number of times.
     * Note: reset will wipe out all existing Corfu data.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param endpoint      endpoint of the server to reset
     * @param retries       Number of retries to bootstrap each node before giving up.
     * @param retryDuration Duration between retries.
     * @param clusterId     Optional cluster Id. If None is provided, then the default one
     *                      will be used to create a base client.
     */
    public static void reset(@NonNull String endpoint,
                             int retries,
                             @NonNull Duration retryDuration,
                             Optional<UUID> clusterId) {
        reboot(endpoint, CorfuRuntimeParameters.builder().build(), retries, retryDuration, true, clusterId);
    }

    /**
     * Resets the given server.
     * Attempts to reset a server finite number of times.
     * Note: reset will wipe out all existing Corfu data.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param endpoint               endpoint of the server to reset
     * @param corfuRuntimeParameters CorfuRuntimeParameters can specify security parameters.
     * @param retries                Number of retries to bootstrap each node before giving up.
     * @param retryDuration          Duration between retries.
     * @param clusterId              Optional cluster Id. If None is provided, then the default one
     *                               will be used to create a base client.
     */
    public static void reset(@NonNull String endpoint,
                             @NonNull CorfuRuntimeParameters corfuRuntimeParameters,
                             int retries,
                             @NonNull Duration retryDuration,
                             Optional<UUID> clusterId) {

        reboot(endpoint, corfuRuntimeParameters, retries, retryDuration, true, clusterId);
    }

    /**
     * Restarts the given server.
     * Attempts to restart a server finite number of times.
     * Note: restart will NOT wipe out any existing data.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param endpoint      endpoint of the server to reset
     * @param retries       Number of retries to bootstrap each node before giving up.
     * @param retryDuration Duration between retries.
     * @param clusterId     Optional cluster Id. If None is provided, then the default one
     *                      will be used to create a base client.
     */
    public static void restart(@NonNull String endpoint,
                               int retries,
                               @NonNull Duration retryDuration,
                               Optional<UUID> clusterId) {

        reboot(endpoint, CorfuRuntimeParameters.builder().build(), retries, retryDuration, false, clusterId);
    }

    /**
     * Restarts the given server.
     * Attempts to restart a server finite number of times.
     * Note: restart will NOT wipe out any existing data.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param endpoint               endpoint of the server to reset
     * @param corfuRuntimeParameters CorfuRuntimeParameters can specify security parameters.
     * @param retries                Number of retries to bootstrap each node before giving up.
     * @param retryDuration          Duration between retries.
     * @param clusterId              Optional cluster Id. If None is provided, then the default one
     *                               will be used to create a base client.
     */
    public static void restart(@NonNull String endpoint,
                               @NonNull CorfuRuntimeParameters corfuRuntimeParameters,
                               int retries,
                               @NonNull Duration retryDuration,
                               Optional<UUID> clusterId) {

        reboot(endpoint, corfuRuntimeParameters, retries, retryDuration, false, clusterId);
    }

    private static void reboot(@NonNull String endpoint,
                               @NonNull CorfuRuntimeParameters corfuRuntimeParameters,
                               int retries,
                               @NonNull Duration retryDuration,
                               boolean resetData,
                               Optional<UUID> clusterId) {

        try (NettyClientRouter router = new NettyClientRouter(NodeLocator.parseString(endpoint),
                corfuRuntimeParameters)) {
            router.addClient(new BaseHandler());
            BaseClient baseClient = new BaseClient(router, Layout.INVALID_EPOCH, clusterId.orElse(DEFAULT_CLUSTER_ID));

            while (retries-- > 0) {
                try {
                    if (resetData) {
                        log.info("Attempting to reset server: {}", endpoint);
                        CFUtils.getUninterruptibly(baseClient.reset());
                    } else {
                        log.info("Attempting to restart server: {}", endpoint);
                        CFUtils.getUninterruptibly(baseClient.restart());
                    }
                    break;
                } catch (Exception e) {
                    log.error("Rebooting node: {} failed with exception:", endpoint, e);
                    if (retries == 0) {
                        throw new RetryExhaustedException("Rebooting node: retry exhausted");
                    }
                    log.warn("Retrying reboot {} times in {}ms.", retries, retryDuration.toMillis());
                    Sleep.sleepUninterruptibly(retryDuration);
                }
            }
            log.info("Successfully rebooted server:{}", endpoint);
        }
    }
}
