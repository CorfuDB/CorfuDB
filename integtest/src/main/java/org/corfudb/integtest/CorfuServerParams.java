package org.corfudb.integtest;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.slf4j.event.Level;

import java.io.FileReader;
import java.util.Optional;

/**
 * Corfu server parameters
 */
@Builder
@Getter
@Slf4j
@ToString
class CorfuServerParams {
    @Builder.Default
    private final ServerMode serverMode = ServerMode.SINGLE;
    @Builder.Default
    private final Level logLevel = Level.DEBUG;
    @Builder.Default
    private final String host = "0.0.0.0";
    private final int port;
    private final Optional<String> managementBootstrap = Optional.empty();
    @Builder.Default
    private final Optional<String> logDir = Optional.empty();
    private final String hostName;

    public String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append(host);

        if (logDir.isPresent()) {
            cmd.append(" -l ").append(logDir.get());
        } else {
            cmd.append(" -m");
        }

        if (serverMode == ServerMode.SINGLE) {
            cmd.append(" -s");
        }

        managementBootstrap.ifPresent(bootstrap -> cmd.append(" -M ").append(bootstrap));

        cmd.append(" -d ").append(logLevel.toString()).append(" ").append(port);

        String cmdLineParams = cmd.toString();
        log.trace("Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }

    public String getAppVersion() throws Exception {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(new FileReader("pom.xml"));
        return model.getParent().getVersion();
    }
}

enum ServerMode {
    SINGLE, CLUSTER
}
