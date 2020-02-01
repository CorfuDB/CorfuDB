package org.corfudb.browser;

import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * This is the CorfuStore Browser Tool which prints data in a given namespace and table.
 *
 * - Created by pmajmudar on 10/16/2019.
 */
@Slf4j
public class CorfuStoreBrowser {
    private final CorfuRuntime runtime;

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     */
    public CorfuStoreBrowser(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Validate that the namespace is not null
     * @param namespace
     */
    private static void verifyNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("Please specify --namespace");
        }
    }

    /**
     * Validate that both namespace and tablename are present
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     */
    private static void verifyNamespaceAndTablename(String namespace, String tableName) {
        verifyNamespace(namespace);
        if (tableName == null) {
            throw new IllegalArgumentException("Please specify --tablename");
        }
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(
        String namespace, String tableName) {
        log.info("Namespace: {}", namespace);
        log.info("TableName: {}", tableName);
        ISerializer dynamicProtobufSerializer =
            new DynamicProtobufSerializer(runtime);
        Serializers.registerSerializer(dynamicProtobufSerializer);

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> corfuTable =
            runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
            })
            .setStreamName(
                TableRegistry.getFullyQualifiedTableName(namespace, tableName))
            .setSerializer(dynamicProtobufSerializer)
            .open();
        return corfuTable;
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    public int printTable(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int size = table.size();
        log.info("======Printing Table {} in namespace {} with {} entries======",
                tablename, namespace, size);
        StringBuilder builder;
        for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry :
            table.entrySet()) {
            builder = new StringBuilder("\nKey:\n" + entry.getKey().getKey())
                .append("\nPayload:\n" + entry.getValue().getPayload())
                .append("\nMetadata:\n" + entry.getValue().getMetadata())
                .append("\n====================\n");
            log.info(builder.toString());
        }
        return size;
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    public int listTables(String namespace)
    {
        verifyNamespace(namespace);
        int numTables = 0;
        log.info("\n=====Tables=======\n");
        for (CorfuStoreMetadata.TableName tableName : runtime.getTableRegistry()
                .listTables(namespace)) {
            log.info("Table: " + tableName.getTableName());
            log.info("Namespace: " + tableName.getNamespace());
            numTables++;
        }
        log.info("\n======================\n");
        return numTables;
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    public int printTableInfo(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        log.info("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int tableSize = table.size();
        log.info("Table {} in namespace {} with ID {} has {} entries",
                tablename, namespace, streamUUID.toString(), tableSize);
        log.info("\n======================\n");
        return tableSize;
    }

    /**
     * Clear the table contents
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table before clearing the table
     */
    public int dropTable(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        log.info("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int tableSize = table.size();
        log.info("Table {} in namespace {} with ID {} with {} entries will be dropped...",
                tablename, namespace, streamUUID.toString(), tableSize);
        table.clear();
        log.info("Table cleared successfully");
        log.info("\n======================\n");
        return tableSize;
    }
}