package org.corfudb.universe.node.server;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.universe.UniverseParams;

import java.io.FileReader;
import java.io.IOException;

@Slf4j
public abstract class AbstractCorfuServer<T extends CorfuServerParams, U extends UniverseParams> implements CorfuServer {

    private static final String POM_FILE = "pom.xml";

    @Getter
    @NonNull
    protected final T params;
    @NonNull
    @Getter
    protected final U universeParams;

    protected AbstractCorfuServer(T params, U universeParams) {
        this.params = params;
        this.universeParams = universeParams;
    }


    /**
     * This method creates a command line string for starting Corfu server
     *
     * @return command line parameters
     */
    protected String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append(getNetworkInterface());

        switch (params.getPersistence()) {
            case DISK:
                cmd.append(" -l ").append(params.getStreamLogDir());
                break;
            case MEMORY:
                cmd.append(" -m");
                break;
        }

        if (params.getMode() == Mode.SINGLE) {
            cmd.append(" -s");
        }

        cmd.append(" -d ").append(params.getLogLevel().toString()).append(" ");

        cmd.append(params.getPort());

        String cmdLineParams = cmd.toString();
        log.trace("Corfu server. Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }

    @Override
    public int compareTo(CorfuServer other) {
        return Integer.compare(getParams().getPort(), other.getParams().getPort());
    }

    /**
     * Provides a current version of this project. It parses the version from pom.xml
     *
     * @return maven/project version
     */
    protected static String getAppVersion() {
        String version = System.getProperty("project.version");
        if (version != null && !version.isEmpty()) {
            return version;
        }

        return parseAppVersionInPom();
    }

    private static String parseAppVersionInPom() {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model;
        try {
            model = reader.read(new FileReader(POM_FILE));
            return model.getParent().getVersion();
        } catch (IOException | XmlPullParserException e) {
            throw new NodeException("Can't parse application version", e);
        }
    }
}
