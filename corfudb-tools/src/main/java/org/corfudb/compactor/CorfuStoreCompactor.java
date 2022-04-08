/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CorfuStoreCompactor {

    private final CorfuRuntime corfuRuntime;
    private final CorfuRuntime cpRuntime;
    private final CorfuStore corfuStore;
    private final String thisNodeUuid;
    private final StringKey previousTokenKey;

    private final int CORFU_LOG_TRIM_ERROR = 2;

    private final boolean enableTrim;
    private final String persistedCacheRoot;

    public CorfuStoreCompactor(CorfuRuntime runtime, CorfuRuntime cpRuntime,
                               boolean enableTrim, String persistedCacheRoot) {
        this.corfuRuntime = runtime;
        this.cpRuntime = cpRuntime;
        this.enableTrim = enableTrim;
        this.persistedCacheRoot = persistedCacheRoot;

        corfuStore = new CorfuStore(corfuRuntime);

        try {
            this.thisNodeUuid = CorfuRuntimeHelper.getThisNodeUuid();
            this.previousTokenKey = StringKey.newBuilder().setKey("previousTokenKey").build();
        } catch(Exception e) {
            throw new RuntimeException("Failed to get node UUID", e);
        }
    }

    private String getCheckpointMapValue(CorfuTable<StringKey, TokenMsg> checkpointMap) {
        return checkpointMap.entrySet().stream()
                .map(entry -> new StringBuilder("{component: ")
                        .append(entry.getKey().getKey().toString())
                        .append(", token: ")
                        .append(entry.getValue())
                        .append("}"))
                .collect(Collectors.joining(", "));
    }

    void checkpoint() {
        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime,
                cpRuntime,
                persistedCacheRoot
        );
        distributedCompactor.startCheckpointing();
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

        final Table<StringKey, TokenMsg, Message> previousTrimTokenTable = CorfuStoreCompactorMain.getPreviousTrimTokenTable();

        final TokenMsg thisTrimToken;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            thisTrimToken = txn.getRecord(previousTrimTokenTable, previousTokenKey).getPayload();
            txn.commit();
        }

        if (thisTrimToken == null) {
            log.warn("Trim token is not present... skipping.");
            return;
        }

        log.info("Previously computed trim token: {}", thisTrimToken);

        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(thisTrimToken.getEpoch(), thisTrimToken.getSequence() - 1L));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        log.info("Trim completed, elapsed({}s), log address up to {} (exclusive).",
                    TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), thisTrimToken.getSequence());

        Table<StringKey, TokenMsg, Message> checkpointMap = CorfuStoreCompactorMain.getCheckpointMap();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            log.info("Current checkpoint map: {}", txn.getRecord(previousTrimTokenTable, previousTokenKey));
            txn.commit();
        }
    }

    public void updateThisNodeTrimToken() {
        log.info("Start to update trim token for node {}.", thisNodeUuid);

        // Get the smallest checkpoint address for trimming that has valid data to trim.
        final Table<StringKey, TokenMsg, Message> checkpointMap = CorfuStoreCompactorMain.getCheckpointMap();
        TokenMsg ckToken;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ckToken = txn.getRecord(checkpointMap, DistributedCompactor.CHECKPOINT_KEY).getPayload();
            txn.commit();
        }
        if (ckToken == null) {
            log.warn("No values in the checkpoint map.");
            return;
        }

        final Table<StringKey, TokenMsg, Message> previousTrimTokenTable = CorfuStoreCompactorMain.getPreviousTrimTokenTable();

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TokenMsg nodeTokenVal = txn.getRecord(checkpointMap, DistributedCompactor.CHECKPOINT_KEY).getPayload();
            if (Objects.equals(nodeTokenVal, ckToken)) {
                // TODO: How to log this to syslog
                log.error(
                        "ERROR: the trim token of node {} hasn't moved forward " +
                                "over the last compaction cycle.", thisNodeUuid);
            }
            txn.putRecord(previousTrimTokenTable, previousTokenKey, ckToken, null);
            txn.commit();
        } catch (TransactionAbortedException tae) {
            log.warn("Another node updated the trim token");
        }

        log.info("New trim token {} is updated for node {}.", ckToken, thisNodeUuid);
    }
}
