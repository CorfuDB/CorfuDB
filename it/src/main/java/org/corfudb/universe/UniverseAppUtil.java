package org.corfudb.universe;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

        try {
            Path path = Paths.get(ClassLoader.getSystemResource("corfu.version").toURI());
            return new String(Files.readAllBytes(path));
        } catch (Exception e) {
            throw new IllegalStateException("Corfu version file not found");
        }
    }
}
