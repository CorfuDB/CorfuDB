package org.corfudb.browser;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        listTags,
        listTablesForTag,
        listTagsForTable,
        listTagsMap,
        printMetadataMap,
        addRecord,
        lrRequestGlobalFullSync
    }

    private static final String USAGE = "Usage: corfu-browser "+
        "--operation=<operation> " +
        "[--host=<host>] [--port=<port>] " +
        "[--offline-db-dir=<dbDir>] " +
        "[--namespace=<namespace>] [--tablename=<tablename>] " +
        "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
        "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
        "[--diskPath=<pathToTempDirForLargeTables>] "+
        "[--numItems=<numItems>] "+
        "[--address=<address>] "+
        "[--batchSize=<itemsPerTransaction>] "+
        "[--itemSize=<sizeOfEachRecordValue>] "
        + "[--keyToEdit=<keyToEdit>] [--newRecord=<newRecord>] [--tag=<tag>]"
        + "[--keyToDelete=<keyToDelete>]"
        + "[--keysToDeleteFilePath=<keysToDeleteFilePath>]"
        + "[--keyToAdd=<keyToAdd>] [--valueToAdd=<valueToAdd>] [--metadataToAdd=<metadataToAdd>]"
        + "[--tlsEnabled=<tls_enabled>]\n"
        + "Options:\n"
        + "--host=<host>   Hostname\n"
        + "--port=<port>   Port\n"
        + "--operation=<listTables|infoTable|showTable|clearTable"
        + "|editTable|deleteRecord|loadTable|listenOnTable|listTags|listTagsMap"
        + "|listTablesForTag|listTagsForTable|listAllProtos"
        + "|lrRequestGlobalFullSync> Operation\n"
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
        + "--keysToDeleteFilePath=<keysToDeleteFilePath> Path to file having all keys in one json array\n"
        + "--newRecord=<newRecord> New Edited record to insert\n"
        + "--keyToAdd=<keyToAdd>"
        + "--valueToAdd=<valueToAdd>"
        + "--metadataToAdd=<metadataToAdd>"
        + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) {
        try {
            mainMethod(args);
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }

    private static String getOperation(Map<String, Object> opts) {
        try {
            String operation = opts.get("--operation").toString();
            return operation;
        } catch (Exception e) {
            return "";
        }
    }

    /* If host parameter found, OnlineBrowser mode
       No host param, looks for offline-db-dir param
     */
    private static String getHostName(Map<String, Object> opts) {
        try {
            String host = opts.get("--host").toString();
            return host;
        } catch (Exception e) {
            return null;
        }
    }

    /* If directory parameter found, OfflineBrowser mode
     */
    private static String getOfflineDBDir(Map<String, Object> opts) {
        try {
            String path = opts.get("--offline-db-dir").toString();
            return path;
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer getPortNumber(Map<String, Object> opts) {
        final int defaultPort = 9000;
        try {
            Integer host = Integer.parseInt(opts.get("--port").toString());
            return host;
        } catch (Exception e) {
            return defaultPort;
        }
    }

    private static Boolean isTLSEnabled(Map<String, Object> opts) {
        final Boolean defaultValue = false;
        try {
            Boolean tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
            return tlsEnabled;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static int mainMethod(String[] args) {
        CorfuBrowserEditorCommands browser;
        log.info("CorfuBrowser starting...");
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String operation = getOperation(opts);
        String offlineDbDir = getOfflineDBDir(opts);
        String host = getHostName(opts);

        Integer port = getPortNumber(opts);
        boolean tlsEnabled = isTLSEnabled(opts);

        if (host != null) {
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

            CorfuRuntime runtime = CorfuRuntime.fromParameters(builder.build());
            String singleNodeEndpoint = String.format("%s:%d", host, port);
            runtime.parseConfigurationString(singleNodeEndpoint);
            log.info("Connecting to corfu cluster at {}", singleNodeEndpoint);
            runtime.connect();
            log.info("Successfully connected to {}", singleNodeEndpoint);

            boolean skipDynamicProtoSerializer = operation.startsWith("lr");
            String diskPath = null;
            if (opts.get("--diskPath") != null) {
                diskPath = opts.get("--diskPath").toString();
            }
            browser = new CorfuStoreBrowserEditor(runtime, diskPath, skipDynamicProtoSerializer);
        } else if (offlineDbDir != null) {
            browser = new CorfuOfflineBrowserEditor(offlineDbDir);
        } else {
            Preconditions.checkArgument(false,
                    "Either host and port must be given or offline-db-dir must be provided");
            browser = null;
        }
        final String namespace = Optional.ofNullable(opts.get("--namespace"))
                .map(Object::toString)
                .orElse(null);
        final String tableName = Optional.ofNullable(opts.get("--tablename"))
                .map(Object::toString)
                .orElse(null);
        final String batchSizeStr = "--batchSize";
        int batchSize = 100000; // this would be the default batch size for corfu transactions

        switch (Enum.valueOf(OperationType.class, operation)) {
            case listTables:
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                return browser.listTables(namespace);
            case infoTable:
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                return browser.printTableInfo(namespace, tableName);
            case clearTable:
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                return browser.clearTable(namespace, tableName);
            case showTable:
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                return browser.printTable(namespace, tableName);
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
                CorfuDynamicRecord record = browser.editRecord(namespace, tableName, keyToEdit, newRecord);
                if (record != null) {
                    return 1;
                }
                break;
            case deleteRecord:
                final String keyToDeleteStr = "--keyToDelete";
                final String keysToDeleteFilePathStr = "--keysToDeleteFilePath";
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                String keyToDelete = null;
                if (opts.get(keyToDeleteStr) != null) {
                    keyToDelete = String.valueOf(opts.get(keyToDeleteStr));
                    Preconditions.checkNotNull(keyToDelete,
                            "Key To Delete is Null.");
                    return browser.deleteRecords(namespace, tableName, Arrays.asList(keyToDelete), batchSize);
                } else if (opts.get(keysToDeleteFilePathStr) != null) {
                    String pathToKeyFile = String.valueOf(opts.get(keysToDeleteFilePathStr));
                    if (opts.get(batchSizeStr) != null) {
                        batchSize = Integer.parseInt(opts.get(batchSizeStr).toString());
                    }
                    return browser.deleteRecordsFromFile(namespace, tableName, pathToKeyFile, batchSize);
                } else {
                    Preconditions.checkNotNull(keyToDelete,
                            "keyToDelete or keysToDeleteFilePath must be specified");
                }
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
                Preconditions.checkNotNull(metadataToAdd,
                        "New Metadata is null");
                CorfuDynamicRecord addedRecord = browser.addRecord(namespace, tableName, keyToAdd,
                        valueToAdd, metadataToAdd);
                return addedRecord != null ? 1: 0;
            case loadTable:
                int numItems = 1000;
                if (opts.get("--numItems") != null) {
                    numItems = Integer.parseInt(opts.get("--numItems").toString());
                }
                if (opts.get(batchSizeStr) != null) {
                    batchSize = Integer.parseInt(opts.get(batchSizeStr).toString());
                } else {
                    batchSize = 1000;
                }
                int itemSize = 1024;
                if (opts.get("--itemSize") != null) {
                    itemSize = Integer.parseInt(opts.get("--itemSize").toString());
                }
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                return browser.loadTable(namespace, tableName, numItems, batchSize, itemSize);
            case listenOnTable:
                numItems = Integer.MAX_VALUE;
                if (opts.get("--numItems") != null) {
                    numItems = Integer.parseInt(opts.get("--numItems").toString());
                }
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                return browser.listenOnTable(namespace, tableName, numItems);
            case listTags:
                Set<String> streamTagSet = browser.listStreamTags();
                return streamTagSet.size();
            case listTablesForTag:
                if (opts.get("--tag") != null) {
                    String streamTag = opts.get("--tag").toString();
                    List<CorfuStoreMetadata.TableName> tablesForTag = browser.listTablesForTag(streamTag);
                    return tablesForTag.size();
                } else {
                    log.warn("The '--tag' flag was not specified. Displaying all streams tags and their tables.");
                    Map<String, List<CorfuStoreMetadata.TableName>> tablesForTag = browser.listTagToTableMap();
                    return tablesForTag.size();
                }
            case listTagsForTable:
                Preconditions.checkArgument(isValid(namespace),
                        "Namespace is null or empty.");
                Preconditions.checkArgument(isValid(tableName),
                        "Table name is null or empty.");
                final Set<String> tagsForTable = browser.listTagsForTable(namespace, tableName);
                return tagsForTable.size();
            case listTagsMap:
                final Map<String, List<CorfuStoreMetadata.TableName>> tagToTableMap = browser.listTagToTableMap();
                return tagToTableMap.size();
            case listAllProtos:
                return browser.printAllProtoDescriptors();
            case printMetadataMap:
                if (opts.get("--address") != null) {
                    long address = Integer.parseInt(opts.get("--address").toString());
                    EnumMap<IMetadata.LogUnitMetadataType, Object> metaMap = browser.printMetadataMap(address);
                    return metaMap.size();
                } else {
                    log.error("Print metadata map for a specific address. Specify using tag --address");
                }
            case lrRequestGlobalFullSync:
                browser.requestGlobalFullSync();
                return 0;
            default:
                break;
        }
        return -1;
    }

    private static boolean isValid(final String str) {
        return str != null && !str.isEmpty();
    }
}
