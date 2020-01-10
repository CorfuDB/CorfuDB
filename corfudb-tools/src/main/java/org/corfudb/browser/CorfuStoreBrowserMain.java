package org.corfudb.browser;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * Main class for the CorfuStore Browser Tool.
 * Command line options are documented in the USAGE variable.
 *
 * - Created by pmajmudar on 10/16/2019
 */
@Slf4j
public class CorfuStoreBrowserMain {
    private static final String USAGE = "Usage: corfu-browser --host=<host> " +
        "--port=<port> --namespace=<namespace> --tablename=<tablename> " +
        "--operation=<operation> "+
        "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
        "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
        "[--tlsEnabled=<tls_enabled>]\n"
        + "Options:\n"
        + "--host=<host>   Hostname\n"
        + "--port=<port>   Port\n"
        + "--operation=<showTables|listTables> Operation\n"
        + "--namespace=<namespace>   Namespace\n"
        + "--tablename=<tablename>   Table Name\n"
        + "--keystore=<keystore_file> KeyStore File\n"
        + "--ks_password=<keystore_password> KeyStore Password\n"
        + "--truststore=<truststore_file> TrustStore File\n"
        + "--truststore_password=<truststore_password> Truststore Password\n"
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
            CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder
                builder = CorfuRuntime.CorfuRuntimeParameters.builder()
                .cacheDisabled(true)
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

            CorfuStoreBrowser browser = new CorfuStoreBrowser(runtime);
            switch (operation) {
                case "listTables":
                    String namespace = opts.containsKey("--namespace") ?
                        opts.get("--namespace").toString() : null;
                    browser.listTables(namespace);
                    break;

                default:
                    CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                        browser.getTable(opts.get("--namespace").toString(),
                        opts.get("--tablename").toString());
                    browser.printTable(table);
            }
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }
}
