package org.corfudb.cmdlets;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
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
    void main(String[] args);

    default void configureBase(Map<String, Object> opts)
    {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        switch ((String)opts.get("--log-level"))
        {
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

    default CorfuRuntime configureRuntime(Map<String,Object> opts)
    {
        return new CorfuRuntime()
                .parseConfigurationString((String)opts.get("--config"))
                .connect();
    }

    default Set<UUID> streamsFromString(String streamString)
    {
        if (streamString == null) { return Collections.emptySet(); }
        return Pattern.compile(",")
                .splitAsStream(streamString)
                .map(String::trim)
                .map(UUID::fromString)
                .collect(Collectors.toSet());
    }
}
