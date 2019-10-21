package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.CorfuRuntimeHelper;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * CorfuStoreCompactorMain is an application which runs and checkpoints and trims all CorfuStore data at regular
 * intervals.
 * NOTE: This only checkpoints CorfuStore streams. Any other streams in the system will encounter a Trimmed Exception.
 * Created by zlokhandwala on 11/5/19.
 */
@Slf4j
public class CorfuStoreCompactorMain {
    private static final String USAGE = "Usage: corfu-compactor --host=<host> --port=<port> "
            + "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] "
            + "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] "
            + "[--tlsEnabled=<tls_enabled>] --interval=<interval>\n"
            + "Options:\n"
            + "--host=<host>                                Hostname.\n"
            + "--port=<port>                                Port.\n"
            + "--keystore=<keystore_file>                   KeyStore File.\n"
            + "--ks_password=<keystore_password>            KeyStore Password.\n"
            + "--truststore=<truststore_file>               TrustStore File.\n"
            + "--truststore_password=<truststore_password>  Truststore Password.\n"
            + "--tlsEnabled=<tls_enabled>                   Enable TLS.\n"
            + "--interval=<interval>                        Interval in minutes.";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Parse the options given, using docopt.
        Map<String, Object> opts = new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
        Duration interval = Duration.ofMinutes(Long.parseLong(opts.get("--interval").toString()));

        CorfuRuntime runtime = CorfuRuntimeHelper.getRuntimeForArgs(opts);
        CorfuStoreCompactor compactor = new CorfuStoreCompactor(runtime, interval);
        ScheduledFuture future = compactor.initialize();

        try {
            future.get();
        } catch (Exception exception) {
            log.error("Error in Browser Execution.", exception);
            throw exception;
        }
    }
}
