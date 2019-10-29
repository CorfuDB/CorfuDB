package org.corfudb.universe.universe.vm;

import org.corfudb.common.result.Result;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Loads config data from properties files.
 */
public class VmConfigPropertiesLoader {

    private VmConfigPropertiesLoader() {
        //prevent creating instances
    }

    public static final String VM_PROPERTIES_FILE = "vm.properties";
    public static final String VM_CREDENTIALS_PROPERTIES_FILE = "vm.credentials.properties";

    public static Result<Properties, PropsLoaderException> loadVmProperties() {
        return Result.of(() -> loadPropertiesFile(VM_PROPERTIES_FILE));
    }

    public static Result<Properties, PropsLoaderException> loadVmCredentialsProperties() {
        return Result.of(() -> loadPropertiesFile(VM_CREDENTIALS_PROPERTIES_FILE));
    }

    private static Properties loadPropertiesFile(String propertiesFile) {
        Properties credentials = new Properties();
        URL credentialsUrl = ClassLoader.getSystemResource(propertiesFile);
        try (InputStream is = credentialsUrl.openStream()) {
            credentials.load(is);
        } catch (IOException e) {
            throw new PropsLoaderException("Can't load credentials", e);
        }
        return credentials;
    }

    public static class PropsLoaderException extends RuntimeException {
        public PropsLoaderException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
