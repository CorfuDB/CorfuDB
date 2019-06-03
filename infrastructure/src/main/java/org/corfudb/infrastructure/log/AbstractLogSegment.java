package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.LogData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.channels.FileChannel;
import java.util.Iterator;

/**
 * TODO: add javadoc.
 *
 * Created by WenbinZhu on 5/28/19.
 */
@Getter
@RequiredArgsConstructor
public abstract class AbstractLogSegment implements AutoCloseable {

    final long startAddress;

    @NonNull
    final String fileName;

    @NonNull
    final FileChannel writeChannel;

    @NonNull
    final FileChannel readChannel;

    // TODO: persist revision in header.
    // Used for checking equality between stream log and garbage log segment in recovery.
    long compactRevision;

    // public abstract LogData read(long address);

    public abstract void append(long address, LogData entry);

    // public Iterator<LogData> iterator() {
    //     throw new NotImplementedException();
    // }

    public abstract void close();
}
