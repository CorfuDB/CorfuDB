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
@Getter
public abstract class AbstractCorfuServer<T extends CorfuServerParams, U extends UniverseParams>
        implements CorfuServer {

    @NonNull
    protected final T params;

    @NonNull
    protected final U universeParams;

    @NonNull
    protected final String version;

    protected AbstractCorfuServer(
            @NonNull T params, @NonNull U universeParams, @NonNull String version) {
        this.params = params;
        this.universeParams = universeParams;
        this.version = version;
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
            default:
                throw new IllegalStateException("Unknown persistence mode");
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
}
