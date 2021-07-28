package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.primitives.Bytes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.utils.KeyEncodingUtil;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.LATEST_VERSION_READ;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_COLUMN_SUFFIX;

/**
 * The DatabaseHandler provides an interface for the RocksDB instance storing data for server side
 * RemoteCorfuTables.
 *
 * <p>Created by nvaishampayan517 on 7/23/21.
 */
@Slf4j
public class DatabaseHandler implements AutoCloseable {

    //Loads the C++ library backing RocksDB
    static {
        RocksDB.loadLibrary();
    }

    private RocksDB database;
    private ThreadPoolExecutor threadPoolExecutor;
    private final Map<byte[], ColumnFamilyHandle> columnFamilies;

    /**
     * Constructor for the database handler. Each server should have a single database handler to serve all requests
     * to the underlying database.
     * @param dataPath The filepath for the database files.
     * @param options The configuration options to start the database.
     * @param threadPoolExecutor The thread pool to serve client requests.
     */
    public DatabaseHandler(@NonNull Path dataPath, @NonNull Options options,
                           @NonNull ThreadPoolExecutor threadPoolExecutor) {
        try {
            RocksDB.destroyDB(dataPath.toFile().getAbsolutePath(), options);
            this.database = RocksDB.open(options, dataPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.threadPoolExecutor = threadPoolExecutor;

        //Must be initialized as an empty map and updated through the add table function
        this.columnFamilies = new Hashtable<>();
    }
    //TODO: make all core functionality private and expose methods that perform the operations through the thread pool

    /**
     * This function provides an interface to read a key from the database.
     * @param encodedKey The key to read.
     * @param streamID The stream backing the database to read from.
     * @return A byte[] containing the value mapped to the key, or null if the key is associated to a null value.
     * @throws RocksDBException An error raised in scan.
     */
    public byte[] get(byte[] encodedKey, byte[] streamID) throws RocksDBException {
        final byte[] returnVal;
        if (!columnFamilies.containsKey(streamID)) {
            //TODO: create explicit stream read up to latest timestamp
        }
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID));
        iter.seek(encodedKey);
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                //We've hit some error in iteration
                log.error("Error in RocksDB get operation:", e);
                throw e;
            } finally {
                iter.close();
            }
            //We've reached the end of the data
            //TODO: add read from log stream check
            returnVal = null;
        } else {
            if (Arrays.equals(KeyEncodingUtil.extractEncodedKey(iter.key()),
                    KeyEncodingUtil.extractEncodedKey(encodedKey))) {
                //This works due to latest version first comparator
                returnVal = iter.value();
            } else {
                //TODO: add read from log stream check
                returnVal = null;
            }
        }
        iter.close();
        return returnVal;
    }

    /**
     * This function provides an interface to add a key to the database.
     *
     * @param encodedKey The key to write to.
     * @param value The value to write to the key.
     * @param streamID The stream backing the database that the key will be added to.
     * @throws RocksDBException A database error on put.
     */
    public void update(byte[] encodedKey, byte[] value, byte[] streamID) throws RocksDBException {
        try {
            database.put(columnFamilies.get(streamID), encodedKey, value);
            byte[] metadataColumn = Bytes.concat(streamID,METADATA_COLUMN_SUFFIX);
            database.put(columnFamilies.get(metadataColumn), LATEST_VERSION_READ,
                    KeyEncodingUtil.extractTimestampAsByteArray(encodedKey));
        } catch (RocksDBException e) {
            log.error("Error in RocksDB put operation:", e);
            throw e;
        }
    }

    /**
     * This function provides an interface to delete a range of keys from the database.
     *
     * @param encodedKeyBegin The start of the range (inclusive).
     * @param encodedKeyEnd The end of the range (exclusive).
     * @param streamID The stream backing the database from which the keys are deleted.
     * @throws RocksDBException A database error on delete.
     */
    public void delete(byte[] encodedKeyBegin, byte[] encodedKeyEnd, byte[] streamID) throws RocksDBException {
        try {
            database.deleteRange(columnFamilies.get(streamID), encodedKeyBegin, encodedKeyEnd);
        } catch (RocksDBException e) {
            log.error("Error in RocksDB delete operation:", e);
            throw e;
        }
    }

    /**
     * This function performs a cursor scan of the first 20 elemenmts of the database.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(byte[] streamID, long timestamp) throws RocksDBException {
        return scan(20,streamID, timestamp);
    }

    /**
     * This function performs a cursor scan of the database from the start.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(int numEntries, byte[] streamID, long timestamp) throws RocksDBException {
        ReadOptions iterOptions = new ReadOptions().setTotalOrderSeek(true);
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID), iterOptions);
        iter.seekToFirst();
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                log.error("Error in RocksDB scan operation:", e);
                throw e;
            } finally {
                iter.close();
                iterOptions.close();
            }
            //otherwise, the database is empty
            return new LinkedList<>();
        }  else {
            //begin at first value -> extract prefix and add timestamp to it
            byte[] startingPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
            byte[] startingKey = KeyEncodingUtil.constructDatabaseKey(startingPrefix, timestamp);
            iter.close();
            iterOptions.close();
            return scanInternal(startingKey,numEntries,streamID, false);
        }
    }

    /**
     * This function performs a cursor scan of the database, returning 20 entries (if possible).
     * @param encodedKeyBegin The starting point of the scan.
     * @param streamID The stream backing the database to scan.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(byte[] encodedKeyBegin, byte[] streamID) throws RocksDBException {
        return scan(encodedKeyBegin, 20, streamID);
    }

    /**
     * This function provides an interface for a cursor scan of the database.
     * @param encodedKeyBegin The starting point of the scan.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(byte[] encodedKeyBegin, int numEntries, byte[] streamID) throws RocksDBException {
        //TODO: address double counting issue -> need to skip first value when restarting from previous scan
        return scanInternal(encodedKeyBegin, numEntries, streamID, true);
    }

    private List<byte[][]> scanInternal(byte[] encodedKeyBegin, int numEntries, byte[] streamID, boolean skipFirst) throws RocksDBException {
        KeyEncodingUtil.VersionedKey start = KeyEncodingUtil.extractVersionedKey(encodedKeyBegin);
        long timestamp = start.getTimestamp();
        ReadOptions iterOptions = new ReadOptions().setTotalOrderSeek(true);
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID), iterOptions);
        byte[] prevPrefix = start.getEncodedRemoteCorfuTableKey();
        byte[] currPrefix;
        boolean encounteredPrefix = false;
        iter.seek(encodedKeyBegin);
        List<byte[][]> results = new LinkedList<>();
        int elements = 0;
        while (iter.isValid()) {
            currPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
            if (Arrays.equals(currPrefix, prevPrefix)) {
                if (!encounteredPrefix) {
                    if (!skipFirst) {
                        results.add(new byte[][]{iter.key(), iter.value()});
                        elements++;

                    } else {
                        skipFirst = false;
                    }
                    encounteredPrefix = true;
                }
                iter.next();
            } else {
                byte[] nextKey = KeyEncodingUtil.constructDatabaseKey(currPrefix, timestamp);
                iter.seek(nextKey);
                prevPrefix = currPrefix;
                encounteredPrefix = false;
            }
            if (elements >= numEntries) {
                break;
            }
        }
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                log.error("Error in RocksDB delete operation:", e);
                throw e;
            } finally {
                iter.close();
                iterOptions.close();
            }
            //Otherwise, we have simply hit the end of the database
        }
        iter.close();
        iterOptions.close();
        return results;
    }

    /**
     * This function provides an interface to check if a key is present in the table.
     * Equivalent to (get(encodedKey, streamID) != null)
     * @param encodedKey The key to check.
     * @param streamID The stream backing the table to check.
     * @return True, if a non-null value exists in the table for the given key and timestamp.
     * @throws RocksDBException An error occuring in the search.
     */
    public boolean containsKey(byte[] encodedKey, byte[] streamID) throws RocksDBException {
        try {
            byte[] val = get(encodedKey, streamID);
            return val != null;
        } catch (RocksDBException e) {
            log.error("Error in RocksDB containsKey operation:", e);
            throw e;
        }
    }

    /**
     * This function provides an interface to check if a value is present in the table.
     * @param encodedValue The value to check.
     * @param streamID The stream backing the table to check.
     * @param timestamp The timestamp of the check request.
     * @param scanSize The size of each cursor scan to the database.
     * @return True, if the given value exists in the table.
     * @throws RocksDBException An error occuring in the search.
     */
    public boolean containsValue(byte[] encodedValue, byte[] streamID, long timestamp, int scanSize) throws RocksDBException {
        boolean first = true;
        byte[] lastKey = null;
        List<byte[][]> scannedEntries;
        do {
            if (first) {
                scannedEntries = scan(streamID, timestamp);
                first = false;
            } else {
                scannedEntries = scan(lastKey,scanSize, streamID);
            }

            for (byte[][] scannedEntry : scannedEntries) {
                if (Arrays.equals(scannedEntry[1], encodedValue)) {
                    return true;
                }
            }
            lastKey = scannedEntries.get(scannedEntries.size()-1)[0];
        } while (scannedEntries.size() >= scanSize);
        return false;
    }

    //TODO: when writing async versions of these methods, count needs to be incremented atomically
    public int size(byte[] streamID, long timestamp, int scanSize) throws RocksDBException {
        boolean first = true;
        byte[] lastKey = null;
        int count = 0;
        List<byte[][]> scannedEntries;
        do {
            if (first) {
                scannedEntries = scan(streamID, timestamp);
                first = false;
            } else {
                scannedEntries = scan(lastKey,scanSize, streamID);
            }

            count += scannedEntries.size();

            lastKey = scannedEntries.get(scannedEntries.size()-1)[0];
        } while (scannedEntries.size() >= scanSize);
        return count;
    }

    /**
     * This function releases all resources held by C++ objects backing the RocksDB objects.
     */
    @Override
    public void close() {
        for (ColumnFamilyHandle handle : columnFamilies.values()) {
            handle.close();
        }
        database.close();
    }
}
