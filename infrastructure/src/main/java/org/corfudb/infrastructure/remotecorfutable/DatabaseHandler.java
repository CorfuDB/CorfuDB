package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.errorprone.annotations.DoNotCall;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.Positive;
import org.corfudb.infrastructure.remotecorfutable.utils.KeyEncodingUtil;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.security.Key;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.EMPTY_VALUE;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.INVALID_DATABASE_KEY_MSG;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.INVALID_STREAM_ID_MSG;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.LATEST_VERSION_READ;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_COLUMN_CACHE_SIZE;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_COLUMN_SUFFIX;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.isEmpty;
import static org.corfudb.infrastructure.remotecorfutable.utils.KeyEncodingUtil.validateDatabaseKey;

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

    private final RocksDB database;
    //instead of using this use completable future
    private final ThreadPoolExecutor threadPoolExecutor;
    private final long SHUTDOWN_TIMEOUT;
    private final Map<UUID, ColumnFamilyHandlePair> columnFamilies;

    /**
     * Constructor for the database handler. Each server should have a single database handler to serve all requests
     * to the underlying database.
     * @param dataPath The filepath for the database files.
     * @param options The configuration options to start the database.
     * @param threadPoolExecutor The thread pool to serve client requests.
     * @param SHUTDOWN_TIMEOUT The amount of time to await termination of threadPoolExecutor
     */
    public DatabaseHandler(@NonNull Path dataPath, @NonNull Options options,
                           @NonNull ThreadPoolExecutor threadPoolExecutor, @NonNull long SHUTDOWN_TIMEOUT) {
        try {
            RocksDB.destroyDB(dataPath.toFile().getAbsolutePath(), options);
            this.database = RocksDB.open(options, dataPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.threadPoolExecutor = threadPoolExecutor;

        //Must be initialized as an empty map and updated through the add table function
        this.columnFamilies = new ConcurrentHashMap<>();
        this.SHUTDOWN_TIMEOUT = SHUTDOWN_TIMEOUT;
    }

    /**
     * Adds a ColumnFamilyHandle to the database representing the specific table. Should only be called through the
     * stream listener.
     * @param streamID The table to add to the database.
     * @throws RocksDBException An error in adding the database table.
     */
    public void addTable(@NonNull UUID streamID) throws RocksDBException {
        if (columnFamilies.containsKey(streamID)) {
            return;
        }
        ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
        tableOptions.optimizeUniversalStyleCompaction();
        ComparatorOptions comparatorOptions = new ComparatorOptions();
        ReversedVersionedKeyComparator comparator = new ReversedVersionedKeyComparator(comparatorOptions);
        tableOptions.setComparator(comparator);
        ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(getBytes(streamID), tableOptions);
        ColumnFamilyHandle tableHandle;
        try {
            tableHandle = database.createColumnFamily(tableDescriptor);
        } catch (RocksDBException e) {
            log.error("Error in creating column family for table {}.", streamID);
            log.error("Cause of error: ", e);
            tableOptions.close();
            comparatorOptions.close();
            comparator.close();
            throw e;
        }

        ColumnFamilyOptions metadataOptions = new ColumnFamilyOptions();
        metadataOptions.optimizeForPointLookup(METADATA_COLUMN_CACHE_SIZE);
        ColumnFamilyDescriptor metadataDescriptor = new ColumnFamilyDescriptor(
                getMetadataColumnName(getBytes(streamID)), metadataOptions);

        ColumnFamilyHandle metadataHandle;
        try {
             metadataHandle = database.createColumnFamily(metadataDescriptor);
        } catch (RocksDBException e) {
            log.error("Error in creating metadata column family for table {}.", streamID);
            log.error("Cause of error: ", e);
            tableHandle.close();
            comparatorOptions.close();
            comparator.close();
            try {
                database.dropColumnFamily(tableHandle);
            } catch (RocksDBException r) {
                log.error("Error in dropping column family for table {}.", streamID);
                log.error("Cause of error: ", r);
                throw e;
            } finally {
                metadataOptions.close();
            }
            throw e;
        }
        ColumnFamilyHandlePair addedTables = new ColumnFamilyHandlePair(tableHandle, metadataHandle);
        columnFamilies.put(streamID, addedTables);
    }

    public void removeTable(@NonNull UUID streamID) throws RocksDBException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("REMOVETABLE", INVALID_STREAM_ID_MSG);
        }
        ColumnFamilyHandlePair tablesToClose = columnFamilies.get(streamID);
        tablesToClose.getMetadataTable().close();
        tablesToClose.getStreamTable().close();
        //possible half delete inconsistencies can be addressed in GC
        try {
            database.dropColumnFamily(tablesToClose.getStreamTable());
            database.dropColumnFamily(tablesToClose.getMetadataTable());
        } catch (RocksDBException e) {
            log.error("Error in dropping column families for table {}.", streamID);
            log.error("Cause of error: ", e);
            throw e;
        }

        columnFamilies.remove(streamID);
    }

    /**
     * This function provides an interface to read a key from the database.
     * @param encodedKey The key to read.
     * @param streamID The stream backing the database to read from.
     * @return A byte[] containing the value mapped to the key, or null if the key is associated to a null value.
     * @throws RocksDBException An error raised in scan.
     */
    public byte[] get(byte[] encodedKey, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        final byte[] returnVal;
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("GET", INVALID_STREAM_ID_MSG);
        }
        if (!validateDatabaseKey(encodedKey)) {
            throw new DatabaseOperationException("GET", INVALID_DATABASE_KEY_MSG);
        }
        try (RocksIterator iter = database.newIterator(columnFamilies.get(streamID).getStreamTable())) {
            iter.seek(encodedKey);
            if (!iter.isValid()) {
                iter.status();
                //We've reached the end of the data
                returnVal = null;
            } else {
                if (Arrays.equals(KeyEncodingUtil.extractEncodedKey(iter.key()),
                        KeyEncodingUtil.extractEncodedKey(encodedKey)) && !isEmpty(iter.value())) {
                    //This works due to latest version first comparator
                    returnVal = iter.value();
                } else {
                    returnVal = null;
                }
            }
        }
        return returnVal;
    }

    /**
     * This method performs {@link #get(byte[], UUID)} asynchronously.
     */
    public CompletableFuture<byte[]> getAsync(byte[] encodedKey, @NonNull UUID streamID) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                byte[] val = get(encodedKey,streamID);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB get operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function provides an interface to add a key to the database.
     *
     * @param encodedKey The key to write to.
     * @param value The value to write to the key.
     * @param streamID The stream backing the database that the key will be added to.
     * @throws RocksDBException A database error on put.
     */
    public void update(byte[] encodedKey, byte[] value, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("PUT", INVALID_STREAM_ID_MSG);
        }
        if (!validateDatabaseKey(encodedKey)) {
            throw new DatabaseOperationException("PUT", INVALID_DATABASE_KEY_MSG);
        }
        if (value == null) {
            value = EMPTY_VALUE;
        }
        database.put(columnFamilies.get(streamID).getStreamTable(), encodedKey, value);
        database.put(columnFamilies.get(streamID).getMetadataTable(), LATEST_VERSION_READ,
                KeyEncodingUtil.extractTimestampAsByteArray(encodedKey));

    }

    /**
     * This method performs {@link #update(byte[], byte[], UUID)} asynchronously.
     */
    public CompletableFuture<Void> updateAsync(byte[] encodedKey, byte[] value, @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                update(encodedKey,value,streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB put operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function provides an interface to atomically put multiple values into the database.
     * @param encodedKeyValuePairs The List of key-value pairs to add to the table.
     * @param streamID The stream backing the table the updates are written to.
     * @throws RocksDBException An error in the batched write.
     */
    public void updateAll(@NonNull List<byte[][]> encodedKeyValuePairs,@NonNull UUID streamID)
            throws RocksDBException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("PUTALL", INVALID_STREAM_ID_MSG);
        }
        //encodedKeyValuePairs must be non-empty
        ColumnFamilyHandle currTable = columnFamilies.get(streamID).getStreamTable();
        ColumnFamilyHandle metadataTable = columnFamilies.get(streamID).getMetadataTable();
        try (WriteBatch batchedWrite = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            for (byte[][] encodedKeyValuePair : encodedKeyValuePairs) {
                byte[] key = encodedKeyValuePair[0];
                if (!validateDatabaseKey(key)) {
                    throw new DatabaseOperationException("PUTALL", INVALID_DATABASE_KEY_MSG);
                }
                byte[] value = encodedKeyValuePair[1];
                if (value == null) {
                    value = EMPTY_VALUE;
                }
                batchedWrite.put(currTable,key,value);
            }
            batchedWrite.put(metadataTable, LATEST_VERSION_READ,
                    KeyEncodingUtil.extractTimestampAsByteArray(encodedKeyValuePairs.get(0)[0]));
            database.write(writeOptions,batchedWrite);
        }
    }

    /**
     * This method performs {@link #updateAll(List, UUID)} asynchronously.
     */
    public CompletableFuture<Void> updateAllAsync(@NonNull List<byte[][]> encodedKeyValuePairs,
                                                  @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                updateAll(encodedKeyValuePairs,streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB put all operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function provides an interface to clear the entire table at a specific version,
     * which is performed by atomically inputting null values at every key in the table.
     * @param streamID The stream backing the table to clear.
     * @param timestamp The timestamp at which the clear was requested.
     * @throws RocksDBException An error in writing all updates.
     */
    public void clear(@NonNull UUID streamID, long timestamp) throws RocksDBException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("CLEAR", INVALID_STREAM_ID_MSG);
        }
        //no data race on cached value since only mutates allowed are add/remove to columnFamilies
        //if streamID is removed during write batch creation, write will fail, so no inconsistency
        ColumnFamilyHandle currTable = columnFamilies.get(streamID).getStreamTable();
        ColumnFamilyHandle metadataTable = columnFamilies.get(streamID).getMetadataTable();
        byte[] keyPrefix;
        byte[] prevPrefix = EMPTY_VALUE;
        try (RocksIterator iter = database.newIterator(currTable);
             WriteBatch batchedWrite = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            iter.seekToFirst();
            while (iter.isValid()) {
                keyPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
                if (Arrays.equals(keyPrefix, prevPrefix)) {
                    iter.next();
                    continue;
                }
                //put empty "null" value at every key in the table
                batchedWrite.put(currTable,KeyEncodingUtil.constructDatabaseKey(keyPrefix, timestamp), EMPTY_VALUE);
                //seek will move iterator to value after the "next key" if it does not exist
                iter.seek(KeyEncodingUtil.constructDatabaseKey(keyPrefix, 0L));
                prevPrefix = keyPrefix;
            }
            iter.status();
            batchedWrite.put(metadataTable,LATEST_VERSION_READ, Longs.toByteArray(timestamp));
            database.write(writeOptions,batchedWrite);
        }
    }

    /**
     * This method performs {@link #clear(UUID, long)} asynchronously.
     */
    public CompletableFuture<Void> clearAsync(@NonNull UUID streamID, long timestamp) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                clear(streamID, timestamp);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB clear operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function provides an interface to delete a range of keys from the database.
     * encodedKeyBegin must be less than encodedKeyEnd with the comparator used (reverse lexicographic)
     *
     * @param encodedKeyBegin The start of the range.
     * @param encodedKeyEnd The end of the range.
     * @param includeLastKey If true, delete encodedKeyEnd from the database as well.
     * @param streamID The stream backing the database from which the keys are deleted.
     * @throws RocksDBException A database error on delete.
     */
    public void delete(byte[] encodedKeyBegin, byte[] encodedKeyEnd,
                       boolean includeLastKey, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("DELETE", INVALID_STREAM_ID_MSG);
        }
        if (!validateDatabaseKey(encodedKeyBegin) || !validateDatabaseKey(encodedKeyEnd)) {
            throw new DatabaseOperationException("DELETE", INVALID_DATABASE_KEY_MSG);
        }
        ColumnFamilyHandle currTable = columnFamilies.get(streamID).getStreamTable();
        try (WriteBatch batchedWrite = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()){
            batchedWrite.deleteRange(currTable, encodedKeyBegin, encodedKeyEnd);
            if (includeLastKey) {
                batchedWrite.delete(currTable, encodedKeyEnd);
            }
            database.write(writeOptions, batchedWrite);
        }
    }

    /**
     * This method performs {@link #delete(byte[], byte[], boolean, UUID)} asynchronously.
     */
    public CompletableFuture<Void> deleteAsync(byte[] encodedKeyBegin, byte[] encodedKeyEnd, boolean includeLastKey,
                                               @NonNull UUID streamID) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                delete(encodedKeyBegin, encodedKeyEnd, includeLastKey, streamID);
                result.complete(null);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB delete operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function performs a cursor scan of the first 20 elemenmts of the database.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(@NonNull UUID streamID, long timestamp)
            throws RocksDBException, DatabaseOperationException {
        return scan(20,streamID, timestamp);
    }

    /**
     * This method performs {@link #scan(UUID, long)} asynchronously.
     */
    public CompletableFuture<List<byte[][]>> scanAsync(@NonNull UUID streamID, long timestamp) {
        return scanAsync(20, streamID, timestamp);
    }

    /**
     * This function performs a cursor scan of the database from the start.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @param timestamp The version of the table to view.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(@Positive int numEntries, @NonNull UUID streamID,
                               long timestamp) throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("SCAN", INVALID_STREAM_ID_MSG);
        }
        try (ReadOptions iterOptions = new ReadOptions()){
            iterOptions.setTotalOrderSeek(true);
            try (RocksIterator iter = database.newIterator(columnFamilies.get(streamID).getStreamTable(), iterOptions)) {
                iter.seekToFirst();
                if (!iter.isValid()) {
                    iter.status();
                    //otherwise, the database is empty
                    return new LinkedList<>();
                } else {
                    //begin at first value -> extract prefix and add timestamp to it
                    byte[] startingPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
                    byte[] startingKey = KeyEncodingUtil.constructDatabaseKey(startingPrefix, timestamp);
                    return scanInternal(startingKey, numEntries, streamID, false);
                }
            }
        }
    }

    /**
     * This method performs {@link #scan(int, UUID, long)} asynchronously.
     */
    public CompletableFuture<List<byte[][]>> scanAsync(@Positive int numEntries,@NonNull UUID streamID,
                                                       long timestamp) {
        CompletableFuture<List<byte[][]>> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                List<byte[][]> val = scan(numEntries, streamID, timestamp);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB scan operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    /**
     * This function performs a cursor scan of the database, returning 20 entries (if possible).
     * @param encodedKeyBegin The starting point of the scan.
     * @param streamID The stream backing the database to scan.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(byte[] encodedKeyBegin, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        if (!validateDatabaseKey(encodedKeyBegin)) {
            throw new DatabaseOperationException("SCAN", INVALID_DATABASE_KEY_MSG);
        }
        return scan(encodedKeyBegin, 20, streamID);
    }

    /**
     * This method performs {@link #scan(byte[], UUID)} asynchronously.
     */
    public CompletableFuture<List<byte[][]>> scanAsync(byte[] encodedKeyBegin, @NonNull UUID streamID) {
        return scanAsync(encodedKeyBegin, 20, streamID);
    }

    /**
     * This function provides an interface for a cursor scan of the database.
     * @param encodedKeyBegin The starting point of the scan.
     * @param numEntries The number of results requested.
     * @param streamID The stream backing the database to scan.
     * @return A list of results from the scan.
     * @throws RocksDBException An error occuring in iteration.
     */
    public List<byte[][]> scan(byte[] encodedKeyBegin, @Positive int numEntries, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        if (!validateDatabaseKey(encodedKeyBegin)) {
            throw new DatabaseOperationException("SCAN", INVALID_DATABASE_KEY_MSG);
        }
        return scanInternal(encodedKeyBegin, numEntries, streamID, true);
    }

    /**
     * This method performs {@link #scan(byte[], int, UUID)} asynchronously.
     */
    public CompletableFuture<List<byte[][]>> scanAsync(byte[] encodedKeyBegin, @Positive int numEntries,
                                                       @NonNull UUID streamID) {
        CompletableFuture<List<byte[][]>> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                List<byte[][]> val = scan(encodedKeyBegin, numEntries, streamID);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB scan operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    private List<byte[][]> scanInternal(byte[] encodedKeyBegin, int numEntries, UUID streamID, boolean skipFirst)
            throws RocksDBException, DatabaseOperationException {
        if (!columnFamilies.containsKey(streamID)) {
            throw new DatabaseOperationException("SCAN", INVALID_STREAM_ID_MSG);
        }
        KeyEncodingUtil.VersionedKey start = KeyEncodingUtil.extractVersionedKey(encodedKeyBegin);
        long timestamp = start.getTimestamp();
        byte[] prevPrefix = start.getEncodedRemoteCorfuTableKey();
        byte[] currPrefix;
        boolean encounteredPrefix = false;
        List<byte[][]> results = new LinkedList<>();
        int elements = 0;
        try (ReadOptions iterOptions = new ReadOptions()){
            iterOptions.setTotalOrderSeek(true);
            try (RocksIterator iter = database.newIterator(columnFamilies.get(streamID).getStreamTable(),
                    iterOptions)) {
                iter.seek(encodedKeyBegin);
                while (iter.isValid()) {
                    currPrefix = KeyEncodingUtil.extractEncodedKey(iter.key());
                    if (!Arrays.equals(currPrefix, prevPrefix)) {
                        byte[] nextKey = KeyEncodingUtil.constructDatabaseKey(currPrefix, timestamp);
                        iter.seek(nextKey);
                        prevPrefix = currPrefix;
                        encounteredPrefix = false;
                    } else if (encounteredPrefix) {
                        iter.next();
                    } else if (isEmpty(iter.value())) {
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
                    iter.status();
                    //Otherwise, we have simply hit the end of the database
                }
                if (skipFirst) {
                    results.remove(0);
                } else if (results.size() == numEntries + 1) {
                    results.remove(results.size() - 1);
                }
            }
        }
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
    public boolean containsKey(byte[] encodedKey, @NonNull UUID streamID)
            throws RocksDBException, DatabaseOperationException {
        byte[] val = get(encodedKey, streamID);
        return val != null;
    }

    /**
     * This method performs {@link #containsKey(byte[], UUID)} asynchronously.
     */
    public CompletableFuture<Boolean> containsKeyAsync(byte[] encodedKey, @NonNull UUID streamID) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                boolean val = containsKey(encodedKey, streamID);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB scan operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
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
    public boolean containsValue(byte[] encodedValue, @NonNull UUID streamID,
                                 long timestamp, @Positive int scanSize)
            throws RocksDBException, DatabaseOperationException {
        boolean first = true;
        byte[] lastKey = null;
        List<byte[][]> scannedEntries = null;
        do {
            if (first) {
                scannedEntries = scan(streamID, timestamp);
                first = false;
            } else {
                lastKey = scannedEntries.get(scannedEntries.size()-1)[0];
                scannedEntries = scan(lastKey,scanSize, streamID);
            }

            for (byte[][] scannedEntry : scannedEntries) {
                if (Arrays.equals(scannedEntry[1], encodedValue)) {
                    return true;
                }
            }
        } while (scannedEntries.size() >= scanSize);
        return false;
    }

    /**
     * This method performs {@link #containsValue(byte[], UUID, long, int)} asynchronously.
     */
    public CompletableFuture<Boolean> containsValueAsync(byte[] encodedValue, @NonNull UUID streamID,
                                                         long timestamp, @Positive int scanSize) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                boolean val = containsValue(encodedValue, streamID, timestamp, scanSize);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB scan operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
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
    public int size(@NonNull UUID streamID, long timestamp, @Positive int scanSize)
            throws RocksDBException, DatabaseOperationException {
        boolean first = true;
        byte[] lastKey = null;
        int count = 0;
        List<byte[][]> scannedEntries = null;
        do {
            if (first) {
                scannedEntries = scan(streamID, timestamp);
                first = false;
            } else {
                lastKey = scannedEntries.get(scannedEntries.size()-1)[0];
                scannedEntries = scan(lastKey,scanSize, streamID);
            }
            count += scannedEntries.size();
        } while (scannedEntries.size() >= scanSize);
        return count;
    }

    /**
     * This method performs {@link #size(UUID, long, int)} asynchronously.
     */
    public CompletableFuture<Integer> sizeAsync(@NonNull UUID streamID, long timestamp,
                                                @Positive int scanSize) {
        CompletableFuture<Integer> result = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                int val = size(streamID, timestamp, scanSize);
                result.complete(val);
            } catch (RocksDBException|DatabaseOperationException e) {
                log.error("Error in RocksDB scan operation: ", e);
                result.completeExceptionally(e);
            }
        }, threadPoolExecutor);
        return result;
    }

    private byte[] getBytes(UUID id) {
        return Bytes.concat(Longs.toByteArray(id.getMostSignificantBits()),
                Longs.toByteArray(id.getLeastSignificantBits()));
    }

    private byte[] getMetadataColumnName(byte[] streamTable) {
        return Bytes.concat(streamTable, METADATA_COLUMN_SUFFIX);
    }

    /**
     * This function releases all resources held by C++ objects backing the RocksDB objects.
     */
    @Override
    public void close() {
        threadPoolExecutor.shutdown();
        try {
            threadPoolExecutor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.debug("Database Handler executor awaitTermination interrupted.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        for (ColumnFamilyHandlePair handle : columnFamilies.values()) {
            handle.getStreamTable().close();
            handle.getMetadataTable().close();
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
    protected List<byte[][]> fullDatabaseScan(UUID streamID) {
        List<byte[][]> allEntries = new LinkedList<>();
        try (RocksIterator iter = database.newIterator(columnFamilies.get(streamID).getStreamTable())) {
            iter.seekToFirst();
            while (iter.isValid()) {
                allEntries.add(new byte[][]{iter.key(), iter.value()});
                iter.next();
            }
        }
        return allEntries;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    private class ColumnFamilyHandlePair {
        private final ColumnFamilyHandle streamTable;
        private final ColumnFamilyHandle metadataTable;
    }
}
