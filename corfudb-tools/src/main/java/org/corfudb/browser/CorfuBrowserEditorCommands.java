package org.corfudb.browser;

import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.ICorfuTable;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CorfuBrowserEditorCommands {
    /**
     * Print ILogData metadata map for a given address
     *
     * @param address specific address to read metadata map from
     * @return
     */
    EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address);

    /**
     * Fetches the table from the given namespace
     *
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    ICorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(
            String namespace, String tableName);

    /**
     * Prints the payload and metadata in the given table
     *
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    int printTable(String namespace, String tablename);

    /**
     * List all tables in CorfuStore
     *
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    int listTables(String namespace);

    /**
     * Print information about a specific table in CorfuStore
     *
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    int printTableInfo(String namespace, String tablename);

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    int printAllProtoDescriptors();

    /**
     * Clear the table contents
     *
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table before clearing the table
     */
    int clearTable(String namespace, String tablename);

    /**
     * Add a record in a table and namespace
     *
     * @param namespace   namespace of the table
     * @param tableName   name of the table
     * @param newKey      JSON string representing the key to add
     * @param newValue    JSON string representing the value to add
     * @param newMetadata JSON string representing the metadata to add
     * @return CorfuDynamicRecord the newly added record.  null if no record
     * was created
     */
    CorfuDynamicRecord addRecord(String namespace, String tableName,
                                 String newKey, String newValue,
                                 String newMetadata);

    /**
     * Edit a record in a table and namespace
     *
     * @param namespace namespace of the table
     * @param tableName name of the table
     * @param keyToEdit JSON string representing the key whose corresponding
     *                  record is to be editted
     * @param newRecord JSON string representing the new value to be inserted
     *                  against keyToEdit
     * @return CorfuDynamicRecord the edited CorfuDynamicRecord.  null if no
     * record was edited, either due to an error or key not found.
     */
    CorfuDynamicRecord editRecord(String namespace, String tableName,
                                  String keyToEdit, String newRecord);


    /**
     *
     * @param namespace namespace this table resides in
     * @param tableName name of the table
     * @param pathToKeysFile path to the file where all the keys to be deleted are
     * @param batchSize number of deletions to be put in one transaction
     * @return number of keys actually deleted
     */
    int deleteRecordsFromFile(String namespace, String tableName, String pathToKeysFile, int batchSize);

    /**
     * Delete a record in a table and namespace
     *
     * @param namespace   namespace of the table
     * @param tableName   name of the table
     * @param keysToDelete a list of JSON strings of the keys to be deleted
     * @return number of keys deleted.
     */
    int deleteRecords(String namespace, String tableName, List<String> keysToDelete, int batchSize);

    /**
     * Loads the table with random data
     *
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     * @param numItems  - total number of items to load
     * @param batchSize - number of items in each transaction
     * @param itemSize  - size of each item - a random string array
     * @return - number of entries in the table
     */
    int loadTable(String namespace, String tableName, int numItems, int batchSize, int itemSize);

    /**
     * Subscribe to and just dump the updates read from a table
     *
     * @param namespace namespace to listen on
     * @param tableName tableName to subscribe to
     * @param stopAfter number of updates to stop listening at
     * @return number of updates read so far
     */
    int listenOnTable(String namespace, String tableName, int stopAfter);

    /**
     * List all stream tags present in the Registry.
     *
     * @return stream tags
     */
    Set<String> listStreamTags();

    /**
     * List a map of stream tags to table names.
     *
     * @return map of tags to table names in the registry
     */
    Map<String, List<CorfuStoreMetadata.TableName>> listTagToTableMap();

    /**
     * List all tags for the given table.
     *
     * @param namespace namespace for the table of interest
     * @param table     table name of interest
     */
    Set<String> listTagsForTable(String namespace, String table);

    /**
     * List all tables with a specific stream tag.
     *
     * @param streamTag specific stream tag, if empty or null return all stream tags map
     * @return table names with given 'streamTag'
     */
    List<CorfuStoreMetadata.TableName> listTablesForTag(@Nonnull String streamTag);

    /**
     * LR API: Request full sync for all remote sites
     */
    void requestGlobalFullSync();
}
