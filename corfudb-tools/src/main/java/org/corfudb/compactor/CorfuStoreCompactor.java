/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class CorfuStoreCompactor {

    private final CorfuRuntime corfuRuntime;
    private final CorfuRuntime cpRuntime;
    private final String thisNodeUuid;

    private final byte PROTOBUF_SERIALIZER_CODE = (byte) 25;

    private final int CORFU_LOG_TRIM_ERROR = 2;

    private final boolean enableTrim;
    private final boolean isLeader;
    private final String persistedCacheRoot;

    private final int txRetries = 5;

    public CorfuStoreCompactor(CorfuRuntime runtime, CorfuRuntime cpRuntime,
                               boolean enableTrim, String persistedCacheRoot, boolean isLeader) {
        this.corfuRuntime = runtime;
        this.cpRuntime = cpRuntime;
        this.enableTrim = enableTrim;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isLeader = isLeader;

        try {
            this.thisNodeUuid = CorfuRuntimeHelper.getThisNodeUuid();
        } catch(Exception e) {
            throw new RuntimeException("Failed to get node UUID", e);
        }
    }

    private CorfuTable<String, Token> getCheckpointMap() {
        return CorfuRuntimeHelper.getCheckpointMap(corfuRuntime);
    }

    private CorfuTable<String, Token> getNodeTrimTokenMap() {
        return CorfuRuntimeHelper.getNodeTrimTokenMap(corfuRuntime);
    }

    private String getCheckpointMapValues(CorfuTable<String, Token> checkpointMap) {
        return checkpointMap.entrySet().stream()
                .sorted((Comparator<Map.Entry<?, Token>>) (o1, o2) -> o1.getValue().compareTo(o2.getValue()))
                .map(entry -> new StringBuilder("{component: ")
                        .append(entry.getKey())
                        .append(", token: ")
                        .append(entry.getValue())
                        .append("}"))
                .collect(Collectors.joining(", "));
    }

    void checkpoint() {
        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime,
                cpRuntime,
                persistedCacheRoot,
                isLeader);
        distributedCompactor.runCompactor();
    }

    public void trim() {
        if(!enableTrim) {
            return;
        }

        log.info("enable trim is true, performing prefixTrim");
        try {
            trimLog();
        } catch (WrongEpochException wee) {
            // TODO: Find a way to log this to syslog
            log.error(
                    "Compactor: Trim failed, ignore the WrongEpochException" +
                            CORFU_LOG_TRIM_ERROR, wee);
        } catch (Throwable t) {
            String msg = "Compactor: Trim failed!";
            // TODO: Find a way to log this to syslog with error code
            log.error(msg, CORFU_LOG_TRIM_ERROR, t);
            throw new RuntimeException(msg, t);
        }
    }

    // Following stuff mostly duplicates stuff from FrameworkCompactor
    // refactor to somewhere common and share or merge or something

    /**
     * Perform log-trimming on CorfuDB by selecting the smallest value from checkpoint map.
     */
    private void trimLog() {
        log.info("Starting CorfuStore trimming task");

        final CorfuTable<String, Token> nodeTrimTokenMap = getNodeTrimTokenMap();
        final Token thisNodeTrimToken = nodeTrimTokenMap.get(thisNodeUuid);

        if (thisNodeTrimToken == null) {
            log.warn("Trim token is not present... skipping.");
            return;
        }

        log.info("Previously computed trim token: {}", thisNodeTrimToken);

        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(thisNodeTrimToken.getEpoch(), thisNodeTrimToken.getSequence() - 1L));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        log.info("Trim completed, elapsed({}s), log address up to {} (exclusive).",
                    TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), thisNodeTrimToken.getSequence());
        log.info("Current checkpoint map: {}", this.getCheckpointMapValues(getCheckpointMap()));
    }

    public void updateThisNodeTrimToken() {
        log.info("Start to update trim token for node {}.", thisNodeUuid);

        // Get the smallest checkpoint address for trimming that has valid data to trim.
        final CorfuTable<String, Token> checkpointMap = getCheckpointMap();
        Token minToken = new Token(Long.MAX_VALUE, Long.MAX_VALUE);
        boolean foundMinToken = false;
        for (Map.Entry<String, Token> ck: checkpointMap.entrySet()) {
            // While upgrading from old setups we might have some roles with no data which will never
            // really get checkpointed. So while picking the min, let's pick the smallest non-zero value.
            if (ck.getValue().getSequence() == Address.NON_ADDRESS) {
                log.warn("Ignoring {} as it has no data to lose", ck.getKey());
                continue;
            }
            if (ck.getValue().compareTo(minToken) < 0) {
                minToken = ck.getValue();
                foundMinToken = true;
            }
        }

        if (!foundMinToken) {
            log.warn("No values in the checkpoint map.");
            return;
        }

        final CorfuTable<String, Token> nodeTrimTokenMap = getNodeTrimTokenMap();

        if (Objects.equals(nodeTrimTokenMap.get(thisNodeUuid), minToken)) {
            // TODO: How to log this to syslog
            log.error(
                    "ERROR: the trim token of node {} hasn't moved forward " +
                            "over the last compaction cycle.", thisNodeUuid);
        }

        nodeTrimTokenMap.put(thisNodeUuid, minToken);

        log.info("New trim token {} is updated for node {}.", minToken, thisNodeUuid);
    }
}
