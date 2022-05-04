/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.exceptions.UnreachableClusterException;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

@Slf4j
public class CorfuRuntimeHelper {

    private CorfuRuntime corfuRuntime;

    private static int maxWriteSize = Integer.MAX_VALUE;

    private int bulkReadSize;

    private volatile String runtimeKeyStore;
    private volatile String runtimeKeystorePasswordFile;
    private volatile String runtimeTrustStore;
    private volatile String runtimeTrustStorePasswordFile;
    private volatile boolean enableTls;

    public static final String CHECKPOINT = "checkpoint";

    private static final int systemDownHandlerTriggerLimit = 100;  // Corfu default is 20
    private static final Runnable defaultSystemDownHandler = new Runnable(){
        @Override
        public void run() {
            throw new UnreachableClusterException("Cluster is unavailable");
        }
    };

    CorfuRuntimeHelper(List<String> hostnames, int port, int maxWriteSize, int bulkReadSize) {
        this.enableTls = false;
        this.bulkReadSize = bulkReadSize;
        this.maxWriteSize = maxWriteSize;
        log.info("Set maxWriteSize to {}, bulkReadSize to {}, no tls", maxWriteSize, bulkReadSize);
        connectCorfuRuntime(hostnames, port);
    }

    CorfuRuntimeHelper(List<String> hostnames, int port, int maxWriteSize, int bulkReadSize,
                       String runtimeKeyStore,
                       String runtimeKeystorePasswordFile,
                       String runtimeTrustStore,
                       String runtimeTrustStorePasswordFile) {
        this.maxWriteSize = maxWriteSize;
        this.bulkReadSize = bulkReadSize;
        this.runtimeKeyStore = runtimeKeyStore;
        this.runtimeKeystorePasswordFile = runtimeKeystorePasswordFile;
        this.runtimeTrustStore = runtimeTrustStore;
        this.runtimeTrustStorePasswordFile = runtimeTrustStorePasswordFile;
        this.enableTls = true;
        log.info("Set maxWriteSize to {}, bulkReadSize to {} with TLS", maxWriteSize, bulkReadSize);
        connectCorfuRuntime(hostnames, port);
    }

    private void connectCorfuRuntime(List<String> hostnames, int port) {
        CorfuRuntimeParameters params = buildCorfuRuntimeParameters();
        String connectionString = constructConnectionString(hostnames, port);
        corfuRuntime = CorfuRuntime.fromParameters(params);
        corfuRuntime.parseConfigurationString(connectionString).connect();
        log.info("Successfully connected to {}", hostnames);
    }

    private CorfuRuntimeParameters buildCorfuRuntimeParameters() {
        CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder()
            .cacheDisabled(true)
            .priorityLevel(PriorityLevel.HIGH)
            .maxWriteSize(maxWriteSize)
            .bulkReadSize(bulkReadSize)
            .systemDownHandler(defaultSystemDownHandler)
            .systemDownHandlerTriggerLimit(systemDownHandlerTriggerLimit);

        if (enableTls) {
            enableCorfuTls(builder);
        } else {
            builder.tlsEnabled(false);
        }

        return builder.build();
    }

    private void enableCorfuTls(CorfuRuntimeParametersBuilder corfuRuntimeParametersBuilder) {
        corfuRuntimeParametersBuilder
            .tlsEnabled(true)
            .keyStore(runtimeKeyStore)
            .ksPasswordFile(runtimeKeystorePasswordFile)
            .trustStore(runtimeTrustStore)
            .tsPasswordFile(runtimeTrustStorePasswordFile);
    }

    CorfuRuntime getRuntime() {
        return corfuRuntime;
    }

    private String constructConnectionString(List<String> hostnames, int port) {
        StringBuilder connectionString = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++) {
            connectionString.append(hostnames.get(i) + ":" + port).append(",");
        }
        return connectionString.deleteCharAt(connectionString.length() - 1).toString();
    }
}
