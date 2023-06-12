package org.corfudb.universe;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class UniverseAppUtil {

    /**
     * Provides a current version of this project. It parses the version from pom.xml
     *
     * @return maven/project version
     */
    public String getAppVersion() {
        String version = System.getProperty("project.version");
        if (version != null && !version.isEmpty()) {
            return version;
        }

        log.info("getting input stream");

        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream("corfu.version")) {
            if (inputStream == null) {
                throw new IllegalStateException("Corfu version file not found");
            }
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Corfu version file not found");
        }
    }
}
