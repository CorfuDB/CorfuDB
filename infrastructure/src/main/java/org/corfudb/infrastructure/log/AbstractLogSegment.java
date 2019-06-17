package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * TODO: add javadoc.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
@Getter
public abstract class AbstractLogSegment implements AutoCloseable {

    final long ordinal;

    @NonNull
    final String filePath;

    @NonNull
    final StreamLogParams logParams;

    @NonNull
    final SegmentMetaData segmentMetaData;

    @NonNull
    FileChannel writeChannel;

    @NonNull
    FileChannel readChannel;

    @NonNull
    FileChannel scanChannel;

    // TODO: persist revision in header.
    // Used for checking equality between stream log segment and garbage log segment in recovery.
    long compactRevision;

    AbstractLogSegment(long ordinal, String fileName,
                       StreamLogParams logParams,
                       SegmentMetaData segmentMetaData) {
        this.ordinal = ordinal;
        this.filePath = fileName;
        this.logParams = logParams;
        this.segmentMetaData = segmentMetaData;

        try {
            this.writeChannel = getChannel(fileName, false);
            this.readChannel = getChannel(fileName, true);
            this.scanChannel = getChannel(fileName, true);
        } catch (IOException ioe) {
            log.error("Error opening file {}", fileName, ioe);
            IOUtils.closeQuietly(writeChannel);
            IOUtils.closeQuietly(readChannel);
            IOUtils.closeQuietly(scanChannel);
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Close the segment, releasing the resources if any.
     */
    public abstract void close();

    /**
     * Opens the file channel to manipulate the file, creating a new file if
     * it does not exist. If the readOnly parameter is set, the channel will
     * be opened in read-only mode.
     *
     * @param filePath the path of the file to open
     * @param readOnly if the file should be opened only for read
     * @return the file channel to manipulate the specified file
     */
    private FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        if (readOnly) {
            if (!new File(filePath).exists()) {
                throw new FileNotFoundException(filePath);
            }
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ)
            );
        }

        try {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW);
            FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

            // First time creating this segment file, need to sync the parent directory
            File segFile = new File(filePath);
            syncDirectory(segFile.getParent());
            return channel;

        } catch (FileAlreadyExistsException ex) {
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE)
            );
        }
    }
}
