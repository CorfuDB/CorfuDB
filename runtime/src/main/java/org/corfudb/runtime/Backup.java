package org.corfudb.runtime;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.Utility;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class Backup {
    String filePath;
    List<UUID> streamIDs;
    long timestamp;
    CorfuRuntime runtime;

    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) {
        this.filePath = filePath;
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.timestamp = runtime.getAddressSpaceView().getCommittedTail();
    }

    public boolean start() throws IOException {
        /**
         * make a tmp directory under the filePath
         */
        if (backup() == false) {
            cleanup();
            return false;
        }

        generateTarFile();
        cleanup();
        return true;
    }

    /**
     * All temp backupTable files will be put at filePath/tmp directory.
     * @return
     */
    public boolean backup() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = filePath + "_" + streamId + "_" + timestamp;
            if (!backupTable(fileName, streamId, runtime, timestamp)) {
                return false;
            }
        }

        return true;
    }

    /**
     * If th log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to backupTable, it will create a file with empty contents.
     * @param fileName
     * @param uuid
     */
    public static boolean backupTable(String fileName, UUID uuid, CorfuRuntime runtime, long timestamp) throws IOException, TrimmedException {
        FileOutputStream fileOutput = new FileOutputStream(fileName);
        ObjectOutputStream objectOutput = new ObjectOutputStream(fileOutput);

        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream stream = (new OpaqueStream(runtime, runtime.getStreamsView().get(uuid, options))).streamUpTo(timestamp);
        Iterator iterator = stream.iterator();

        long numEntries = 0;
        while (iterator.hasNext()) {
            OpaqueEntry lastEntry = (OpaqueEntry) iterator.next();
            List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
            if (smrEntries != null) {
                Map<UUID, List<SMREntry>> map = new HashMap<>();
                map.put(uuid, smrEntries);
                OpaqueEntry newOpaqueEntry = new OpaqueEntry(lastEntry.getVersion(), map);
                ByteBuf byteBuf = Unpooled.buffer(Utility.calculateSize(smrEntries));
                OpaqueEntry.serialize(byteBuf, newOpaqueEntry);
                objectOutput.writeObject(newOpaqueEntry);
                numEntries++;
            }
        }

        objectOutput.close();
        return true;
    }

    /**
     * All generated files under tmp directory will be composed into one .tar file
     */
    public void generateTarFile() {

    }

    /**
     * It is called when it failed to backupTable one of the table, the whole backupTable process will fail and
     * cleanup the files under the tmp directory.
     */
    public void cleanup() {

    }
}
