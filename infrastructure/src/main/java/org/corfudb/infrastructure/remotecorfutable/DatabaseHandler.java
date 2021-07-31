package org.corfudb.infrastructure.remotecorfutable;

import com.google.errorprone.annotations.DoNotCall;
import com.google.protobuf.ByteString;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.Positive;
import org.corfudb.infrastructure.remotecorfutable.utils.KeyEncodingUtil;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.LATEST_VERSION_READ;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.DATABASE_CHARSET;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_COLUMN_CACHE_SIZE;
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
    //instead of using this use completable future
    private ThreadPoolExecutor threadPoolExecutor;
    private final Map<ByteString, ColumnFamilyHandle> columnFamilies;

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
        this.columnFamilies = new ConcurrentHashMap<>();
    }

    public void addTable(ByteString streamID) throws RocksDBException {
        if (columnFamilies.containsKey(streamID)) {
            return;
        }
        ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
        tableOptions.optimizeUniversalStyleCompaction();
        tableOptions.setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
        ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(streamID.toByteArray(), tableOptions);
        ColumnFamilyHandle tableHandle;
        try {
            tableHandle = database.createColumnFamily(tableDescriptor);
        } catch (RocksDBException e) {
            log.error("Error in creating column family for table {}.", streamID.toString(DATABASE_CHARSET));
            log.error("Cause of error: ", e);
            tableOptions.close();
            throw e;
        }

        ColumnFamilyOptions metadataOptions = new ColumnFamilyOptions();
        metadataOptions.optimizeForPointLookup(METADATA_COLUMN_CACHE_SIZE);
        ColumnFamilyDescriptor metadataDescriptor = new ColumnFamilyDescriptor(
                getMetadataColumnName(streamID).toByteArray(), metadataOptions);

        ColumnFamilyHandle metadataHandle;
        try {
             metadataHandle = database.createColumnFamily(metadataDescriptor);
        } catch (RocksDBException e) {
            log.error("Error in creating metadata column family for table {}.", streamID.toString(DATABASE_CHARSET));
            log.error("Cause of error: ", e);
            tableHandle.close();
            try {
                database.dropColumnFamily(tableHandle);
            } catch (RocksDBException r) {
                log.error("Error in dropping column family for table {}.", streamID.toString(DATABASE_CHARSET));
                log.error("Cause of error: ", r);
                throw e;
            } finally {
                metadataOptions.close();
            }
            throw e;
        }
        //TODO: double check the ordering between this section and the get
        synchronized (columnFamilies) {
         columnFamilies.put(streamID, tableHandle);
         columnFamilies.put(getMetadataColumnName(streamID), metadataHandle);
        }
    }
    //TODO: add aysnc API

    /**
     * This function provides an interface to read a key from the database.
     * @param encodedKey The key to read.
     * @param streamID The stream backing the database to read from.
     * @return A byte[] containing the value mapped to the key, or null if the key is associated to a null value.
     * @throws RocksDBException An error raised in scan.
     */
    //TODO: Refactor with try-with-resources - see Slava's comment
    public byte[] get(byte[] encodedKey, ByteString streamID) throws RocksDBException, DatabaseOperationException {
        final byte[] returnVal;
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("GET", "Invalid stream ID");
        }
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID));
        iter.seek(encodedKey);
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                //We've hit some error in iteration
                log.error("Error in RocksDB get operation:", e);
                iter.close();
                throw e;
            }
            //We've reached the end of the data
            returnVal = null;
        } else {
            if (Arrays.equals(KeyEncodingUtil.extractEncodedKey(iter.key()),
                    KeyEncodingUtil.extractEncodedKey(encodedKey)) && iter.value().length != 0) {
                //This works due to latest version first comparator
                returnVal = iter.value();
            } else {
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
    public void update(byte[] encodedKey, byte[] value, ByteString streamID) throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("PUT", "Invalid stream ID");
        }
        if (value == null) {
            value = new byte[0];
        }
        try {
            database.put(columnFamilies.get(streamID), encodedKey, value);
            ByteString metadataColumn = getMetadataColumnName(streamID);
            database.put(columnFamilies.get(metadataColumn), LATEST_VERSION_READ,
                    KeyEncodingUtil.extractTimestampAsByteArray(encodedKey));
        } catch (RocksDBException e) {
            log.error("Error in RocksDB put operation:", e);
            throw e;
        }
    }

    /**
     * This function provides an interface to clear the entire table at a specific version,
     * which is performed by atomically inputting null values at every key in the table.
     * @param streamID The stream backing the table to clear.
     * @param timestamp The timestamp at which the clear was requested.
     * @throws RocksDBException An error in writing all updates.
     */
    public void clear(ByteString streamID, long timestamp) throws RocksDBException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("PUT", "Invalid stream ID");
        }
        //no data race on cached value since only mutates allowed are add/remove to columnFamilies
        //if streamID is removed during write batch creation, write will fail, so no inconsistency
        ColumnFamilyHandle currTable = columnFamilies.get(streamID);
        byte[] keyPrefix;
        try (RocksIterator iter = database.newIterator(currTable);
             WriteBatch batchedWrite = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            iter.seekToFirst();
            while (iter.isValid()) {
                keyPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
                //put empty "null" value at every key in the table
                batchedWrite.put(currTable,KeyEncodingUtil.constructDatabaseKey(keyPrefix, timestamp), new byte[0]);
                //seek will move iterator to value after the "next key" if it does not exist
                iter.seek(findNextKey(keyPrefix));
            }
            iter.status();
            database.write(writeOptions,batchedWrite);
        } catch (RocksDBException e) {
            log.error("Error in RocksDB clear operation:", e);
            throw e;
        }
    }

    /**
     * This function provides an interface to delete a range of keys from the database.
     * encodedKeyBegin must be less than encodedKeyEnd with the comparator used (REVERSE_BYTEWISE_COMPARATOR)
     *
     * @param encodedKeyBegin The start of the range.
     * @param encodedKeyEnd The end of the range.
     * @param includeFirstKey If true, delete encodedKeyBegin from the database as well.
     * @param includeLastKey If true, delete encodedKeyEnd from the database as well.
     * @param streamID The stream backing the database from which the keys are deleted.
     * @throws RocksDBException A database error on delete.
     */
    public void delete(byte[] encodedKeyBegin, byte[] encodedKeyEnd, boolean includeFirstKey, boolean includeLastKey, ByteString streamID) throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("DELETE", "Invalid stream ID");
        }
        byte[] start;
        byte[] end;
        if (!includeFirstKey) {
            start = findNextKey(encodedKeyBegin);
        } else {
            start = encodedKeyBegin;
        }
        if (includeLastKey) {
            end = findNextKey(encodedKeyEnd);
        } else {
            end = encodedKeyEnd;
        }
        deleteInternal(start, end, streamID);
    }

    private void deleteInternal(byte[] encodedKeyBegin, byte[] encodedKeyEnd, ByteString streamID) throws RocksDBException {
        try {
            //this range deletes from start (inclusive) to end (exclusive)
            database.deleteRange(columnFamilies.get(streamID), encodedKeyBegin, encodedKeyEnd);
        } catch (RocksDBException e) {
            log.error("Error in RocksDB delete operation:", e);
            throw e;
        }
    }

    private byte[] findNextKey(byte[] encodedPrevKey) throws DatabaseOperationException {
        KeyEncodingUtil.VersionedKey currVersionedKey = KeyEncodingUtil.extractVersionedKey(encodedPrevKey);
        if (currVersionedKey.getTimestamp() != 0) {
            return KeyEncodingUtil.constructDatabaseKey(currVersionedKey.getEncodedRemoteCorfuTableKey(), currVersionedKey.getTimestamp()-1);
        }
        boolean replaced = false;
        byte[] nextKeyPrefix = currVersionedKey.getEncodedRemoteCorfuTableKey();
        for (int i = nextKeyPrefix.length-1; (i >= 0 && !replaced); i--) {
            if (nextKeyPrefix[i] != 0) {
                nextKeyPrefix[i] -= 1;
                replaced = true;
            }
        }
        if (!replaced) {
            log.error("Attempted to find next key of null key");
            throw new DatabaseOperationException("Find next key", "null keys are unsupported");
        }
        //in java, bytes are signed, but rocks db stores unsigned bytes so -1L is the largest version possible
        return KeyEncodingUtil.constructDatabaseKey(nextKeyPrefix,-1L);
    }

    /**
     * This function performs a cursor scan of the first 20 elemenmts of the database.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(ByteString streamID, long timestamp) throws RocksDBException, DatabaseOperationException {
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
    public List<byte[][]> scan(int numEntries, ByteString streamID, long timestamp) throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("SCAN", "Invalid stream ID");
        }
        ReadOptions iterOptions = new ReadOptions();
        iterOptions.setTotalOrderSeek(true);
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
    public List<byte[][]> scan(byte[] encodedKeyBegin, ByteString streamID) throws RocksDBException, DatabaseOperationException {
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
    public List<byte[][]> scan(byte[] encodedKeyBegin, int numEntries, ByteString streamID) throws RocksDBException, DatabaseOperationException {
        return scanInternal(encodedKeyBegin, numEntries, streamID, true);
    }

    private List<byte[][]> scanInternal(byte[] encodedKeyBegin, int numEntries, ByteString streamID, boolean skipFirst) throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("SCAN", "Invalid stream ID");
        }
        KeyEncodingUtil.VersionedKey start = KeyEncodingUtil.extractVersionedKey(encodedKeyBegin);
        long timestamp = start.getTimestamp();
        ReadOptions iterOptions = new ReadOptions();
        iterOptions.setTotalOrderSeek(true);
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID), iterOptions);
        byte[] prevPrefix = start.getEncodedRemoteCorfuTableKey();
        byte[] currPrefix;
        boolean encounteredPrefix = false;
        iter.seek(encodedKeyBegin);
        List<byte[][]> results = new LinkedList<>();
        int elements = 0;
        while (iter.isValid()) {
            currPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
            if (!Arrays.equals(currPrefix, prevPrefix)) {
                byte[] nextKey = KeyEncodingUtil.constructDatabaseKey(currPrefix, timestamp);
                iter.seek(nextKey);
                prevPrefix = currPrefix;
                encounteredPrefix = false;
            } else if (encounteredPrefix) {
                iter.next();
            } else if (iter.value().length == 0) {
                encounteredPrefix = true;
                iter.next();
            } else {
                results.add(new byte[][]{iter.key(), iter.value()});
                elements++;
                encounteredPrefix = true;
                iter.next();
            }
            if (elements > numEntries) {
                break;
            }
        }
        if (!iter.isValid()) {
            try {
                iter.status();
            } catch (RocksDBException e) {
                log.error("Error in RocksDB scan operation:", e);
                iter.close();
                iterOptions.close();
                throw e;
            }
            //Otherwise, we have simply hit the end of the database
        }
        if (skipFirst) {
            results.remove(0);
        } else if (results.size() == numEntries+1){
            results.remove(results.size()-1);
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
    public boolean containsKey(byte[] encodedKey, ByteString streamID) throws RocksDBException, DatabaseOperationException {
        try {
            byte[] val = get(encodedKey, streamID);
            return val != null;
        } catch (RocksDBException | DatabaseOperationException e) {
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
    public boolean containsValue(byte[] encodedValue, ByteString streamID, long timestamp, @Positive int scanSize) throws RocksDBException, DatabaseOperationException {
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

    /**
     * This function provides an interface to check the size of the table at a given version.
     * @param streamID The stream backing the table to check.
     * @param timestamp The timestamp of the requested table version.
     * @param scanSize The size of each cursor scan to the database.
     * @return The amount of non-null keys present in the database with version earlier than given timestamp.
     * @throws RocksDBException An error occuring in the search.
     * @throws DatabaseOperationException An error occuring with requested stream.
     */
    //TODO: when writing async versions of these methods, count needs to be incremented atomically
    public int size(ByteString streamID, long timestamp, @Positive int scanSize) throws RocksDBException, DatabaseOperationException {
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

    private ByteString getMetadataColumnName(ByteString streamID) {
        return streamID.concat(METADATA_COLUMN_SUFFIX);
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

    /**
     * FOR DEBUG USE ONLY - WILL SCAN EVERY KEY IN THE DATABASE
     * @param streamID The stream to scan.
     * @return Every key in the database with the specified streamID.
     * @deprecated Use only for debug.
     */
    @DoNotCall
    @Deprecated
    protected List<byte[][]> fullDatabaseScan(ByteString streamID) {
        List<byte[][]> allEntries = new LinkedList<>();
        RocksIterator iter = database.newIterator(columnFamilies.get(streamID));
        iter.seekToFirst();
        while(iter.isValid()) {
            allEntries.add(new byte[][]{iter.key(),iter.value()});
            iter.next();
        }
        iter.close();
        return allEntries;
    }
}
