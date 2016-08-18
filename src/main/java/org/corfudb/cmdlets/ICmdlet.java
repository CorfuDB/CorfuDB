package org.corfudb.cmdlets;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.util.Utils;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/10/15.
 */

public interface ICmdlet {
    String[] main2(String[] args);

    default void configureBase(Map<String, Object> opts) {
        if (CorfuServer.serverRunning_p) {
            return;
        }
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        switch ((String) opts.get("--log-level")) {
            case "ERROR":
                root.setLevel(Level.ERROR);
                break;
            case "WARN":
                root.setLevel(Level.WARN);
                break;
            case "INFO":
                root.setLevel(Level.INFO);
                break;
            case "DEBUG":
                root.setLevel(Level.DEBUG);
                break;
            case "TRACE":
                root.setLevel(Level.TRACE);
                break;
            default:
                root.setLevel(Level.INFO);
                System.out.println("Level " + opts.get("--log-level") + " not recognized, defaulting to level INFO");
        }
        root.debug("Arguments are: {}", opts);
    }

    default CorfuRuntime configureRuntime(Map<String, Object> opts) {
        return new CorfuRuntime()
                .parseConfigurationString((String) opts.get("--config"))
                .connect();
    }

    default void checkEndpoint(String endpoint)
            throws NetworkException {
        // Create a client router and ping.
        String host = endpoint.split(":")[0];
        Integer port = Integer.parseInt(endpoint.split(":")[1]);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.addClient(new BaseClient())
                .start();

        if (!router.getClient(BaseClient.class).pingSync()) {
            throw new NetworkException("Couldn't connect to remote endpoint", endpoint);
        }

        router.stop();
    }

    default UUID getUUIDfromString(String id) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (id == null) {
            return null;
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException iae) {
            UUID o = UUID.nameUUIDFromBytes(id.getBytes());
            root.debug("Mapped name UUID {} to {}", id, o);
            return o;
        }
    }

    default Set<UUID> streamsFromString(String streamString) {
        if (streamString == null) {
            return Collections.emptySet();
        }
        return Pattern.compile(",")
                .splitAsStream(streamString)
                .map(String::trim)
                .map(this::getUUIDfromString)
                .collect(Collectors.toSet());
    }

    class cmdlet {
        protected static String[] ok(String... args) {
            return makeStringArray("OK", args);
        }

        protected static String[] err(String... args) {
            return makeStringArray("ERROR", args);
        }

        private static String[] makeStringArray(String zeroth, String... args) {
            String[] a = new String[args.length + 1];
            for (int i = 0; i < args.length; i++) {
                a[i + 1] = args[i];
            }
            a[0] = zeroth;
            return a;
        }

    }

}
