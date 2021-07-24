package org.corfudb.infrastructure.remotecorfutable;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The DatabaseHandler provides an interface for the RocksDB instance storing data for server side
 * RemoteCorfuTables.
 *
 * <p>Created by nvaishampayan517 on 7/23/21.
 */
@Slf4j
public class DatabaseHandler implements AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB database;
    private ThreadPoolExecutor threadPoolExecutor;
    private final Map<byte[], ColumnFamilyHandle> columnFamilies;

    public DatabaseHandler(@NonNull Path dataPath, @NonNull Options options,
                           @NonNull ThreadPoolExecutor threadPoolExecutor,
                           @NonNull Map<byte[], ColumnFamilyHandle> columnFamilies) {
        try {
            RocksDB.destroyDB(dataPath.toFile().getAbsolutePath(), options);
            this.database = RocksDB.open(options, dataPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.threadPoolExecutor = threadPoolExecutor;
        this.columnFamilies = columnFamilies;
    }

    public byte[] get(byte[] encodedKey, byte[] streamID) {
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID));
        iter.seek(encodedKey);
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                //We've hit some error in iteration
                log.error("Error in RocksDB get operation:", e);
                return null;
            }
            //We've reached the end of the data
            //TODO: add read from log stream check
            return null;
        } else {
            byte[] currKey = iter.key();

            //TODO: replace with a check to see if prefixes of both values are equivalent
            if (true) {
                //This works due to latest version first comparator
                return currKey;
            } else {
                //TODO: add read from log stream check
                return null;
            }
        }
    }


    @Override
    public void close() throws Exception {
        for (ColumnFamilyHandle handle : columnFamilies.values()) {
            handle.close();
        }
        database.close();
    }
}
