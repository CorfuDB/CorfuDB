package org.corfudb.infrastructure.utils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utils class for classes that need to write to the filesystem.
 * <p>
 * Created by maithem on 4/4/18.
 */

public class Persistence {

    /**
     * This method fsyncs a directory. It should be called whenever
     * a new file is created. It is required because syncing the child
     * file doesn't ensure that the entry in the parent directory
     * file has also reached disk. A side effect of not doing this is
     * writes that disappear from the replica.
     *
     * @param dir the directory to be synced
     * @throws IOException
     */
    public static void syncDirectory(String dir) throws IOException {
        Path dirPath = Paths.get(dir);
        try (FileChannel channel = FileChannel.open(dirPath)) {
            channel.force(true);
        }
    }
}
