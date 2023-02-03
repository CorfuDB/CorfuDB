package org.corfudb.compactor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.NodeLocator;
import org.docopt.Docopt;

import java.util.Map;
import java.util.Optional;

/**
 * CorfuCompactorConfig class parses the parameters passed and builds the required CorfuRuntime
 */
@Getter
@Slf4j
public class CompactorBaseConfig {

    // Reduce checkpoint batch size due to disk-based nature and for smaller compactor JVM size
    public static final int DISK_BACKED_DEFAULT_CP_MAX_WRITE_SIZE = 1 << 20;
    public static final int DEFAULT_CP_MAX_WRITE_SIZE = 25 << 20;
    public static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 100;  // Corfu default is 20
    public static final int CORFU_LOG_CHECKPOINT_ERROR = 3;
    public static final String USAGE = "Usage: compactor-runner";
    public static final String OPTIONS = "Options:\n";

    private final Runnable defaultSystemDownHandler = () -> {
        throw new UnreachableClusterException("Cluster is unavailable");
    };

    private Map<String, Object> opts;
    private CorfuRuntimeParameters params;
    private NodeLocator nodeLocator;
    private Optional<String> persistedCacheRoot;

    public CompactorBaseConfig(String[] args, String additionalUsageParams, String additionalOptionsParams) {
        parse(args, additionalUsageParams, additionalOptionsParams);
        buildRuntime();
    }

    public void parse(String[] args, String additionalUsageParams, String additionalOptionsParams) {
        this.opts = new Docopt(cmdLineUsageBuilder(additionalUsageParams, additionalOptionsParams))
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
    }

    public void buildRuntime() {
        String host = getOpt("--hostname").orElseThrow(() -> new IllegalStateException("Empty host"));
        int port = getOpt("--port").map(Integer::parseInt)
                .orElseThrow(() -> new IllegalStateException("Port not defined"));

        this.nodeLocator = NodeLocator.builder().host(host).port(port).build();

        persistedCacheRoot = getOpt("--persistedCacheRoot");

        CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder();

        getOpt("--tlsEnabled").ifPresent(tlsEnabledStr -> {
            boolean tlsEnabled = Boolean.parseBoolean(tlsEnabledStr);
            builder.tlsEnabled(tlsEnabled);
            if (tlsEnabled) {
                builder.keyStore(opts.get("--keystore").toString());
                builder.ksPasswordFile(opts.get("--ks_password").toString());
                builder.trustStore(opts.get("--truststore").toString());
                builder.tsPasswordFile(opts.get("--truststore_password").toString());
            }
        });

        Optional<String> maybeMaxWriteSize = getOpt("--maxWriteSize");
        int maxWriteSize;
        if (maybeMaxWriteSize.isPresent()) {
            maxWriteSize = Integer.parseInt(maybeMaxWriteSize.get());
        } else {
            if (!persistedCacheRoot.isPresent()) {
                // in-memory compaction
                maxWriteSize = DEFAULT_CP_MAX_WRITE_SIZE;
            } else {
                // disk-backed compaction
                maxWriteSize = DISK_BACKED_DEFAULT_CP_MAX_WRITE_SIZE;
            }
        }
        builder.maxWriteSize(maxWriteSize);

        getOpt("--bulkReadSize").ifPresent(bulkReadSizeStr -> {
            builder.bulkReadSize(Integer.parseInt(bulkReadSizeStr));
        });

        builder.clientName(host);
        builder.systemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(defaultSystemDownHandler);

        params = builder
                .priorityLevel(PriorityLevel.HIGH)
                .cacheDisabled(true)
                .build();
    }

    Optional<String> getOpt(String param) {
        if (opts.get(param) != null) {
            return Optional.of(opts.get(param).toString());
        } else {
            return Optional.empty();
        }
    }

    private String cmdLineUsageBuilder(String additionalUsageParams, String additionalOptionsParams) {
        String usage = USAGE +
                CompactorCmdLineHelper.USAGE_PARAMS +
                additionalUsageParams +
                "\n" +
                OPTIONS +
                CompactorCmdLineHelper.OPTIONS_PARAMS + "\n" +
                additionalOptionsParams;
        return usage;
    }

    public static class CompactorCmdLineHelper {
        public static final String USAGE_PARAMS = " --hostname=<host> " +
                "--port=<port> " +
                "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
                "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
                "[--persistedCacheRoot=<pathToTempDirForLargeTables>] " +
                "[--maxWriteSize=<maxWriteSizeLimit>] " +
                "[--bulkReadSize=<bulkReadSize>] " +
                "[--tlsEnabled=<tls_enabled>]";

        public static final String OPTIONS_PARAMS =
                "--hostname=<hostname>   Hostname\n"
                + "--port=<port>   Port\n"
                + "--keystore=<keystore_file> KeyStore File\n"
                + "--ks_password=<keystore_password> KeyStore Password\n"
                + "--truststore=<truststore_file> TrustStore File\n"
                + "--truststore_password=<truststore_password> Truststore Password\n"
                + "--persistedCacheRoot=<pathToTempDirForLargeTables> Path to Temp Dir\n"
                + "--maxWriteSize=<maxWriteSize> Max write size smaller than 2GB\n"
                + "--bulkReadSize=<bulkReadSize> Read size for chain replication\n"
                + "--tlsEnabled=<tls_enabled>";
    }
}
