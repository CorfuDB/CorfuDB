package org.corfudb.migration;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.corfudb.format.Types;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.corfudb.migration.EndpointMigration.modifyBindingIpAddress;

/**
 * This migration tool will migrate the local data store files and the log segment files
 * from version 1 to 2. This migration will include remove an extra delimiter (i.e. redundant magic byte)
 * and add a new field to the metadata type. As a result, checksums will be recomputed.
 *
 * To run this tool, execute the following steps:
 *
 * 1. cd migration; mvn clean install
 * 2. cd target; java -jar migration-0.1-SNAPSHOT-shaded.jar corfuDataDir
 */

public class LogFormat1to2 {

    static final int srcVersion = 1;

    static final int destVersion = 2;

    public static final short RECORD_DELIMITER = 0x4C45;

    public static final int METADATA_SIZE = Types.MetadataV0.newBuilder()
            .setChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    /**
     * Migrates the log segments and data store files.
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

        // migrate log segments
        migrateLUData(corfuDir);
        // migrate data-store files.
        migrateLocalDatastore(corfuDir);
        // Transform the layout state local datastore files.
        modifyBindingIpAddress(corfuDir, oldEndpoint, newEndpoint);
    }

    public static void migrateLocalDatastore(String dirStr) throws IOException {

        Set<String> dsKeyPrefixes = newHashSet("SERVER_EPOCH",
                "LAYOUT",
                "PHASE_1",
                "PHASE_2",
                "LAYOUTS",
                "TAIL_SEGMENT",
                "STARTING_ADDRESS",
                "MANAGEMENT",
                "_NODE_ID");

        File dir = new File(dirStr);
        File[] foundFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                for (String prefix : dsKeyPrefixes) {
                    if (name.startsWith(prefix)) {
                        return true;
                    }
                }
                return false;
            }
        });

        for (File file : foundFiles) {
            migrateDsFile(file.getAbsolutePath());
        }
    }

    public static void migrateDsFile(String path) throws IOException {
        Path srcPath = Paths.get(path);
        Path destPath = Paths.get(path + ".ds");

        byte[] bytes = Files.readAllBytes(srcPath);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + Integer.BYTES);
        buffer.putInt(getChecksum(bytes));
        buffer.put(bytes);
        Files.write(destPath, buffer.array(), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
        Files.delete(srcPath);
    }

    public static void migrateLUData(String dir) throws IOException {
        String luDir = dir + File.separator + "log";
        File path = new File(luDir);
        File[] files = path.listFiles();

        if (files == null) {
            throw new IllegalArgumentException("Invalid directory " + dir);
        }

        for (File file : files) {
            processSegment(file.getAbsolutePath());
        }
    }

    public static void processSegment(String path) throws IOException {

        Path srcPath = Paths.get(path);
        Path destPath = Paths.get(path + ".tmp");

        FileChannel src = FileChannel.open(srcPath, EnumSet.of(StandardOpenOption.READ));

        if (src.size() == 0) {
            src.close();
            return;
        }

        FileChannel dest = FileChannel.open(destPath, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE));

        // Parse header
        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        src.read(buf);
        buf.flip();

        Types.MetadataV0 headerMetadata = Types.MetadataV0.parseFrom(buf.array());

        buf = ByteBuffer.allocate(headerMetadata.getLength());
        src.read(buf);
        buf.flip();

        Types.LogHeader header;

        try {
            header = Types.LogHeader.parseFrom(buf.array());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Can't parse log header for " + path);
        }

        if (header.getVersion() != srcVersion) {
            throw new IllegalStateException("Segment version must be " + srcVersion +
                    " but found " + header.getVersion() + " in file " + path);
        }

        // Write header with the new format
        Types.LogHeader newHeader = header.toBuilder().setVersion(destVersion).build();
        ByteBuffer outBuf = getByteBufferWithMetaData(newHeader);
        dest.write(outBuf);

        // Parse segment entries
        while (src.position() < src.size()) {
            // Read and skip delimiter
            buf = ByteBuffer.allocate(Short.BYTES);
            src.read(buf);
            buf.flip();

            if (buf.getShort() != RECORD_DELIMITER) {
                throw new IllegalStateException("Missing delimiter while parsing " + path);
            }

            Types.MetadataV0 entryMetadata = readMetadata(src);
            ByteBuffer serializedEntry = readSerializedEntry(entryMetadata, src);

            if (entryMetadata.getChecksum() != getChecksum(serializedEntry.array())) {
                throw new IllegalStateException("Data corruption detected while reading " + path);
            }

            // Write the new the entry in the new format
            outBuf = getByteBufferWithMetaData(serializedEntry);
            dest.write(outBuf);
        }

        dest.force(true);
        src.close();
        dest.close();

        Files.move(destPath, srcPath, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }


    static Types.MetadataV0 readMetadata(FileChannel fc) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        fc.read(buf);
        buf.flip();
        return Types.MetadataV0.parseFrom(buf.array());
    }

    static ByteBuffer readSerializedEntry(Types.MetadataV0 entryMetadata, FileChannel fc) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(entryMetadata.getLength());
        fc.read(buf);
        buf.flip();
        return buf;
    }

    static Types.Metadata getMetadata(AbstractMessage message) {
        return Types.Metadata.newBuilder()
                .setPayloadChecksum(getChecksum(message.toByteArray()))
                .setLengthChecksum(getChecksum(message.getSerializedSize()))
                .setLength(message.getSerializedSize())
                .build();
    }

    static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        Types.Metadata metadata = getMetadata(message);

        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize()
                + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    static ByteBuffer getByteBufferWithMetaData(ByteBuffer serialized) {
        Types.Metadata metadata = Types.Metadata.newBuilder()
                .setPayloadChecksum(getChecksum(serialized.array()))
                .setLengthChecksum(getChecksum(serialized.array().length))
                .setLength(serialized.array().length)
                .build();

        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize()
                + serialized.array().length);
        buf.put(metadata.toByteArray());
        buf.put(serialized);
        buf.flip();
        return buf;
    }

    /**
     * Returns checksum used for log.
     *
     * @param bytes data over which to compute the checksum
     * @return checksum of bytes
     */
    static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();
        for (byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    static int getChecksum(int num) {
        Hasher hasher = Hashing.crc32c().newHasher();
        return hasher.putInt(num).hash().asInt();
    }
}
