package org.corfudb.migration;

import static com.google.common.collect.Sets.newHashSet;
import static org.corfudb.migration.LogFormat1to2.getChecksum;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;

/**
 * Endpoint migration tool modifies the layout datastore files to replace a particular endpoint.
 * To maintain consistency this should be run on all nodes of the cluster replacing the required
 * endpoint.
 * NOTE: The Corfu Server should NOT be running when this tool is run to avoid disruption.
 *
 * <p>Created by zlokhandwala on 3/4/18.
 */
public class EndpointMigration {

    /**
     * Data-store files responsible for layout recovery.
     */
    private static final Set<String> layoutStateRecoveryFiles =
            newHashSet("LAYOUT_CURRENT", "MANAGEMENT_LAYOUT");

    /**
     * Modifies the endpoint in the layout datastore files.
     *
     * @param args Accepts the arguments in the following order,
     *             dir:         CorfuDB data directory,
     *             oldEndpoint: Old address the server was binding to,
     *             newEndpoint: New address to which the server will bind to.
     * @throws Exception if migration fails.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("Expected parameters: CorfuDB data directory,"
                    + "old endpoint and new endpoint");
        }

        String corfuDir = args[0];
        String oldEndpoint = args[1];
        String newEndpoint = args[2];

        // Transform the layout state local datastore files.
        modifyBindingIpAddress(corfuDir, oldEndpoint, newEndpoint);
    }

    /**
     * Modifies the ip address in the layout files.
     *
     * @param dirStr      Directory path
     * @param oldEndpoint Old endpoint to be replaced.
     * @param newEndpoint New endpoint.
     */
    static void modifyBindingIpAddress(String dirStr,
                                       String oldEndpoint,
                                       String newEndpoint) throws IOException {
        File dir = new File(dirStr);
        File[] layoutStateFiles = dir.listFiles(
                (dir1, name) -> layoutStateRecoveryFiles.stream().anyMatch(name::startsWith));

        if (layoutStateFiles != null) {
            for (File file : layoutStateFiles) {
                Path path = Paths.get(file.getAbsolutePath());

                byte[] bytes = Files.readAllBytes(path);
                // Skip the first 4 checksum bytes.
                byte[] dataBytes = Arrays.copyOfRange(bytes, Integer.BYTES, bytes.length);

                String data = new String(dataBytes);
                data = data.replace(oldEndpoint, newEndpoint);
                dataBytes = data.getBytes();
                ByteBuffer buffer = ByteBuffer.allocate(dataBytes.length + Integer.BYTES);
                buffer.putInt(getChecksum(data.getBytes()));
                buffer.put(data.replace(oldEndpoint, newEndpoint).getBytes());

                Files.write(path, buffer.array(), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
            }
        }
    }
}
