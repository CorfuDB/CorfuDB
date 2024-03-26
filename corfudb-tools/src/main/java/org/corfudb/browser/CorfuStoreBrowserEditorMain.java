package org.corfudb.browser;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * Main class for the CorfuStore Browser Tool.
 * Command line options are documented in the USAGE variable.
 *
 * - Created by pmajmudar on 10/16/2019
 */
@Slf4j
public class CorfuStoreBrowserEditorMain {
    private enum OperationType {
        listTables,
        loadTable,
        infoTable,
        showTable,
        listenOnTable,
        clearTable,
        listAllProtos,
        deleteRecord,
        editTable,
        addRecord,
        listTags,
        listTablesForTag,
        listTagsForTable,
        listTagsMap,
        printMetadataMap
    }

    private static final String USAGE = "Usage: corfu-browser --host=<host> " +
        "--port=<port> [--namespace=<namespace>] [--tablename=<tablename>] " +
        "--operation=<operation> "+
        "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
        "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
        "[--diskPath=<pathToTempDirForLargeTables>] "+
        "[--numItems=<numItems>] "+
        "[--address=<address>] "+
        "[--batchSize=<itemsPerTransaction>] "+
        "[--itemSize=<sizeOfEachRecordValue>] "
        + "[--keyToEdit=<keyToEdit>] [--newRecord=<newRecord>] [--tag=<tag>]"
        + "[--keyToDelete=<keyToDelete>][--keyToAdd=<keyToAdd>] [--valueToAdd=<valueToAdd>] [--metadataToAdd=<metadataToAdd>]"
        + "[--tlsEnabled=<tls_enabled>]\n"
        + "Options:\n"
        + "--host=<host>   Hostname\n"
        + "--port=<port>   Port\n"
        + "--operation=<listTables|infoTable|showTable|clearTable" +
        "|editTable|addRecord|deleteRecord|loadTable|listenOnTable|listTags|listTagsMap" +
        "|listTablesForTag|listTagsForTable|listAllProtos> Operation\n"
        + "--namespace=<namespace>   Namespace\n"
        + "--tablename=<tablename>   Table Name\n"
        + "--tag=<tag>  Stream tag of interest\n"
        + "--keystore=<keystore_file> KeyStore File\n"
        + "--ks_password=<keystore_password> KeyStore Password\n"
        + "--truststore=<truststore_file> TrustStore File\n"
        + "--truststore_password=<truststore_password> Truststore Password\n"
        + "--diskPath=<pathToTempDirForLargeTables> Path to Temp Dir\n"
        + "--numItems=<numItems> Total Number of items for loadTable\n"
        + "--address=<address> Log global address\n"
        + "--batchSize=<batchSize> Number of records per transaction for loadTable\n"
        + "--itemSize=<itemSize> Size of each item's payload for loadTable\n"
        + "--keyToEdit=<keyToEdit> Key of the record to edit\n"
        + "--keyToDelete=<keyToDelete> Key of the record to be deleted\n"
        + "--newRecord=<newRecord> New Edited record to insert\n"
        + "--keyToAdd=<keyToAdd>\n"
        + "--valueToAdd=<valueToAdd>\n"
        + "--metadataToAdd=<metadataToAdd>\n"
        + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) {
        try {
            CorfuRuntime runtime;
            log.info("CorfuBrowser starting...");
            // Parse the options given, using docopt.
            Map<String, Object> opts =
                new Docopt(USAGE)
                    .withVersion(GitRepositoryState.getRepositoryState().describe)
                    .parse(args);
            String host = opts.get("--host").toString();
            Integer port = Integer.parseInt(opts.get("--port").toString());
            boolean tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled")
                .toString());
            String operation = opts.get("--operation").toString();

            final int SYSTEM_EXIT_ERROR_CODE = 1;
            final int SYSTEM_DOWN_RETRIES = 60;
            CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder
                builder = CorfuRuntime.CorfuRuntimeParameters.builder()
                .cacheDisabled(true)
                .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
                .systemDownHandlerTriggerLimit(SYSTEM_DOWN_RETRIES)
                .priorityLevel(CorfuMessage.PriorityLevel.HIGH)
                .tlsEnabled(tlsEnabled);
            if (tlsEnabled) {
                String keystore = opts.get("--keystore").toString();
                String ks_password = opts.get("--ks_password").toString();
                String truststore = opts.get("--truststore").toString();
                String truststore_password =
                    opts.get("--truststore_password").toString();
                builder.tlsEnabled(tlsEnabled)
                    .keyStore(keystore)
                    .ksPasswordFile(ks_password)
                    .trustStore(truststore)
                    .tsPasswordFile(truststore_password);
            }

            runtime = CorfuRuntime.fromParameters(builder.build());
            String singleNodeEndpoint = String.format("%s:%d", host, port);
            runtime.parseConfigurationString(singleNodeEndpoint);
            log.info("Connecting to corfu cluster at {}", singleNodeEndpoint);
            runtime.connect();
            log.info("Successfully connected to {}", singleNodeEndpoint);

            CorfuStoreBrowserEditor browser;
            if (opts.get("--diskPath") != null) {
                browser = new CorfuStoreBrowserEditor(runtime, opts.get(
                    "--diskPath").toString());
            } else {
                browser = new CorfuStoreBrowserEditor(runtime);
            }
            String namespace = Optional.ofNullable(opts.get("--namespace"))
                    .map(Object::toString)
                    .orElse(null);
            String tableName = Optional.ofNullable(opts.get("--tablename"))
                    .map(Object::toString)
                    .orElse(null);

            switch (Enum.valueOf(OperationType.class, operation)) {
                case listTables:
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    browser.listTables(namespace);
                    break;
                case infoTable:
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.printTableInfo(namespace, tableName);
                    break;
                case clearTable:
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.clearTable(namespace, tableName);
                    break;
                case showTable:
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.printTable(namespace, tableName);
                    break;
                case editTable:
                    String keyToEdit = null;
                    String newRecord = null;
                    if (opts.get("--keyToEdit") != null) {
                        keyToEdit = String.valueOf(opts.get("--keyToEdit"));
                    }
                    if (opts.get("--newRecord") != null) {
                        newRecord = String.valueOf(opts.get("--newRecord"));
                    }
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    Preconditions.checkNotNull(keyToEdit,
                        "Key To Edit is Null.");
                    Preconditions.checkNotNull(newRecord,
                        "New Record is null");
                    browser.editRecord(namespace, tableName, keyToEdit, newRecord);
                    break;
                case addRecord:
                    String keyToAdd = null;
                    String valueToAdd = null;
                    String metadataToAdd = null;

                    String keyArg = "--keyToAdd";
                    String valueArg = "--valueToAdd";
                    String metadataArg = "--metadataToAdd";

                    if (opts.get(keyArg) != null) {
                        keyToAdd = String.valueOf(opts.get(keyArg));
                    }
                    if (opts.get(valueArg) != null) {
                        valueToAdd = String.valueOf(opts.get(valueArg));
                    }
                    if (opts.get(metadataArg) != null) {
                        metadataToAdd = String.valueOf(opts.get(metadataArg));
                    }
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    Preconditions.checkNotNull(keyToAdd,
                        "Key To Add is Null.");
                    Preconditions.checkNotNull(valueToAdd,
                        "New Value is null");
                    CorfuDynamicRecord addedRecord = browser.addRecord(namespace, tableName, keyToAdd,
                        valueToAdd, metadataToAdd);
                    break;
                    //return addedRecord != null ? 1: 0;
                case deleteRecord:
                    String keyToDelete = null;
                    if (opts.get("--keyToDelete") != null) {
                        keyToDelete = String.valueOf(opts.get("--keyToDelete"));
                    }
                    Preconditions.checkArgument(isValid(namespace),
                            "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                            "Table name is null or empty.");
                    Preconditions.checkNotNull(keyToDelete,
                            "Key To Delete is Null.");
                    browser.deleteRecord(namespace, tableName, keyToDelete);
                    break;
                case loadTable:
                    int numItems = 1000;
                    if (opts.get("--numItems") != null) {
                        numItems = Integer.parseInt(opts.get("--numItems").toString());
                    }
                    int batchSize = 1000;
                    if (opts.get("--batchSize") != null) {
                        batchSize = Integer.parseInt(opts.get("--batchSize").toString());
                    }
                    int itemSize = 1024;
                    if (opts.get("--itemSize") != null) {
                        itemSize = Integer.parseInt(opts.get("--itemSize").toString());
                    }
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.loadTable(namespace, tableName, numItems, batchSize, itemSize);
                    break;
                case listenOnTable:
                    numItems = Integer.MAX_VALUE;
                    if (opts.get("--numItems") != null) {
                        numItems = Integer.parseInt(opts.get("--numItems").toString());
                    }
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.listenOnTable(namespace, tableName, numItems);
                    break;
                case listTags:
                    browser.listStreamTags();
                    break;
                case listTablesForTag:
                    if (opts.get("--tag") != null) {
                        String streamTag = opts.get("--tag").toString();
                        browser.listTablesForTag(streamTag);
                    } else {
                        log.warn("The '--tag' flag was not specified. Displaying all streams tags and their tables.");
                        browser.listTagToTableMap();
                    }
                    break;
                case listTagsForTable:
                    Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                    Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                    browser.listTagsForTable(namespace, tableName);
                    break;
                case listTagsMap:
                    browser.listTagToTableMap();
                    break;
                case listAllProtos:
                    browser.printAllProtoDescriptors();
                    break;
                case printMetadataMap:
                    if (opts.get("--address") != null) {
                        long address = Integer.parseInt(opts.get("--address").toString());
                        browser.printMetadataMap(address);
                    } else {
                        log.error("Print metadata map for a specific address. Specify using tag --address");
                    }
                default:
                    break;
            }
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }

    private static boolean isValid(final String str) {
        return str != null && !str.isEmpty();
    }
}
