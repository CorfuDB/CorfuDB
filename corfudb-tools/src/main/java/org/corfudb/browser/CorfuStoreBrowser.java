package org.corfudb.browser;

import com.google.common.reflect.TypeToken;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
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
     * @param table
     */
    public void printTable(
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table) {
        StringBuilder builder;
        for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry :
            table.entrySet()) {
            builder = new StringBuilder("\nKey:\n" + entry.getKey().getKey())
                .append("\nPayload:\n" + entry.getValue().getPayload())
                .append("\nMetadata:\n" + entry.getValue().getMetadata())
                .append("\n====================\n");
            log.info(builder.toString());
        }
    }
}
