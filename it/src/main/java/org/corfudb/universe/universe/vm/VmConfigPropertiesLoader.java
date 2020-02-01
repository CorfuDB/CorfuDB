package org.corfudb.universe.universe.vm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

/**
 * Loads config data from properties files.
 */
@Slf4j
public class VmConfigPropertiesLoader {

    public static final Path VM_PROPERTIES = Paths.get("vm.properties");
    public static final Path VM_CREDENTIALS = Paths.get("vm.credentials.properties");

    private VmConfigPropertiesLoader() {
        //prevent creating instances
    }

    public static Result<Properties, PropsLoaderException> loadVmProperties() {
        return Result.of(() -> loadPropertiesFile(VM_PROPERTIES));
    }

    public static Result<Properties, PropsLoaderException> loadVmCredentialsProperties() {
        return Result.of(() -> loadPropertiesFile(VM_CREDENTIALS));
    }

    private static Properties loadPropertiesFile(Path properties) {
        log.info("Load properties: {}", properties);

        return Optional.ofNullable(ClassLoader.getSystemResource(properties.toString()))
                .map(credentialsUrl -> {
                    Properties credentials = new Properties();
                    try (InputStream is = credentialsUrl.openStream()) {
                        credentials.load(is);
                    } catch (IOException e) {
                        throw new PropsLoaderException("Can't load credentials", e);
                    }
                    return credentials;
                })
                .orElseThrow(() -> new PropsLoaderException("Config not found: " + properties));
    }

    public static class PropsLoaderException extends RuntimeException {
        public PropsLoaderException(String message, Throwable cause) {
            super(message, cause);
        }

        public PropsLoaderException(String message) {
            super(message);
        }
    }
}
