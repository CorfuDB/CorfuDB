package org.corfudb.universe.node.server.process;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.universe.node.server.CorfuServerParams;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Information regarding corfu server directory structure.
 * The default structure is:
 * <pre>
 *  ${corfuDir} [~]
 *  | - ${serverDir}
 *      | - corfu.log
 *      | - corfu-server.jar
 *      | - db
 *          | - stream log files
 *          | - corfu - data store directory, contains paxos files and layouts
 *  </pre>
 */
@Getter
public class CorfuServerPath {

    private static final String SERVER_JAR_NAME = "corfu-server.jar";
    private static final String DEFAULT_LOG_FILE = "corfu.log";

    /**
     * Corfu server directory. By default base directory for any corfu server is a home directory: `~/`
     */
    @NonNull
    private final Path corfuDir;

    /**
     * The directory of a particular instance of a corfu server: ${corfuDir}/${serverParams.getName()}
     */
    private final Path serverDir;
    /**
     * Corfu stream log (database) directory
     */
    private final Path dbDir;
    /**
     * Path to corfu server jar file: ${serverDir}/SERVER_JAR_NAME
     */
    private final Path serverJar;
    /**
     * Corfu server relative path name: ${serverParams.getName()}/SERVER_JAR_NAME
     */
    private final Path serverJarRelativePath;
    /**
     * Corfu log file: ${serverDir}/corfu.log
     */
    private final Path corfuLogFile;

    private final CorfuServerParams params;

    public CorfuServerPath(CorfuServerParams params) {
        this(Paths.get("~"), params);
    }

    public CorfuServerPath(Path corfuDir, CorfuServerParams params) {
        this.corfuDir = corfuDir;
        this.params = params;
        serverDir = corfuDir.resolve(params.getName());
        dbDir = corfuDir.resolve(params.getStreamLogDir());

        corfuLogFile = serverDir.resolve(DEFAULT_LOG_FILE);

        serverJarRelativePath = Paths.get(params.getName(), SERVER_JAR_NAME);
        serverJar = corfuDir.resolve(serverJarRelativePath);
    }
}
