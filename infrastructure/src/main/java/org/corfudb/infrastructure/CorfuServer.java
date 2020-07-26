package org.corfudb.infrastructure;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.LoggerUtil;
import org.corfudb.infrastructure.server.CorfuServerCmdLine;
import org.corfudb.infrastructure.server.CorfuServerManager;
import org.corfudb.infrastructure.server.CorfuServerManager.CorfuServerConfigurator;
import org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState;
import org.corfudb.infrastructure.server.CorfuServerStateMachine;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;


/**
 * This is the new Corfu server single-process executable.
 *
 * <p>The command line options are documented in the USAGE variable.
 *
 * <p>Created by mwei on 11/30/15.
 */

@Slf4j
public class CorfuServer {

    /**
     * Main program entry point.
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        CorfuServerCmdLine cmdLine = new CorfuServerCmdLine(args);
        Map<String, Object> opts = cmdLine.getOpts();
        cmdLine.printStartupMsg();

        String logLevel = ((String) opts.get("--log-level")).toUpperCase();
        LoggerUtil.configureLogger(logLevel);

        log.debug("Started with arguments: {}", opts);

        setupNetwork(opts);
        createServiceDirectory(opts);
        validateMetadataRetention(opts);

        CorfuServerConfigurator cfg = CorfuServerConfigurator.builder()
                .opts(opts)
                .build();

        CorfuServerManager serverManager = CorfuServerManager.builder()
                .currentState(CorfuServerState.INIT)
                .configurator(cfg)
                .instance(Optional.empty())
                .build();

        CorfuServerStateMachine stateMachine = CorfuServerStateMachine.builder()
                .actions(CompletableFuture.completedFuture(serverManager))
                .build();

        stateMachine.next(CorfuServerState.START);
        stateMachine.registerShutdownHook();
        stateMachine.waitForShutdown();
    }

    /**
     * Create the service directory if it does not exist.
     *
     * @param opts Server options map.
     */
    private static void createServiceDirectory(Map<String, Object> opts) {
        if ((Boolean) opts.get("--memory")) {
            return;
        }

        Path serviceDir = Paths.get((String) opts.get("--log-path"));

        if (!serviceDir.toFile().isDirectory()) {
            log.error("Service directory {} does not point to a directory. Aborting.",
                    serviceDir);
            throw new UnrecoverableCorfuError("Service directory must be a directory!");
        }

        Path corfuServiceDirPath = serviceDir.toAbsolutePath().resolve("corfu");
        File corfuServiceDir = corfuServiceDirPath.toFile();
        // Update the new path with the dedicated child service directory.
        opts.put("--log-path", corfuServiceDirPath.toString());
        if (!corfuServiceDir.exists() && corfuServiceDir.mkdirs()) {
            log.info("Created new service directory at {}.", corfuServiceDir);
        }
    }

    private static void setupNetwork(Map<String, Object> opts) {
        // Bind to all interfaces only if no address or interface specified by the user.
        // Fetch the address if given a network interface.
        if (opts.get("--network-interface") != null) {
            opts.put("--address", getAddressFromInterfaceName((String) opts.get("--network-interface")));
            opts.put("--bind-to-all-interfaces", false);
        } else if (opts.get("--address") == null) {
            // Default the address to localhost and set the bind to all interfaces flag to true,
            // if the address and interface is not specified.
            opts.put("--bind-to-all-interfaces", true);
            opts.put("--address", "localhost");
        } else {
            // Address is specified by the user.
            opts.put("--bind-to-all-interfaces", false);
        }
    }

    private static void validateMetadataRetention(Map<String, Object> opts) {
        // Check the specified number of datastore files to retain
        if (Integer.parseInt((String) opts.get("--metadata-retention")) < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }
    }
}
