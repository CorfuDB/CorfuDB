package org.corfudb.benchmark;

import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import com.codahale.metrics.MetricRegistry;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

import java.util.*;

/**
 * Operation of Sequencer, contains query, raw token request, multi stream token request,
 * transaction token request, get stream address space operations.
 */
@Slf4j
public class SequencerOperations extends Operation {
    private Timer queryTimer;
    private Timer rawTimer;
    private Timer multiStreamTimer;
    private Timer txTimer;
    private Timer getStreamSpaceTimer;
    SequencerOperations(String name, CorfuRuntime rt, int numRequest) {
        super(rt);
        shortName = name;
        this.numRequest = numRequest;
        setTimer();
    }

    private void setTimer() {
        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        queryTimer = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "benchmark-query-token");
        rawTimer = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "benchmark-raw-token");
        multiStreamTimer = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "benchmark-multistream-token");
        txTimer = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "benchmark-tx-token");
        getStreamSpaceTimer = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "benchmark-getstreamspace-token");

    }

    private void tokenQuery() {
        for (int i = 0; i < numRequest; i++) {
            try(Timer.Context context = MetricsUtils.getConditionalContext(queryTimer)) {
                TokenResponse tokenResponse = rt.getSequencerView().query();
            }
        }
    }

    private void tokenRaw() {
        for (int i = 0; i < numRequest; i++) {
            try(Timer.Context context = MetricsUtils.getConditionalContext(rawTimer)) {
                rt.getSequencerView().next();
            }
        }
    }

    private void tokenMultiStream() {
        for (int i = 0; i < numRequest; i++) {
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            try(Timer.Context context = MetricsUtils.getConditionalContext(multiStreamTimer)) {
                rt.getSequencerView().next(stream);
            }
        }
    }

    private void tokenTx() {
        for (int i = 0; i < numRequest; i++) {
            UUID transactionID = UUID.nameUUIDFromBytes("transaction".getBytes());
            UUID stream = UUID.randomUUID();
            Map<UUID, Set<byte[]>> conflictMap = new HashMap<>();
            Set<byte[]> conflictSet = new HashSet<>();
            byte[] value = new byte[]{0, 0, 0, 1};
            conflictSet.add(value);
            conflictMap.put(stream, conflictSet);

            Map<UUID, Set<byte[]>> writeConflictParams = new HashMap<>();
            Set<byte[]> writeConflictSet = new HashSet<>();
            byte[] value1 = new byte[]{0, 0, 0, 1};
            writeConflictSet.add(value1);
            writeConflictParams.put(stream, writeConflictSet);

            TxResolutionInfo conflictInfo = new TxResolutionInfo(transactionID,
                    new Token(0, -1),
                    conflictMap,
                    writeConflictParams);
            try(Timer.Context context = MetricsUtils.getConditionalContext(txTimer)) {
                rt.getSequencerView().next(conflictInfo, stream);
            }
        }
    }

    private void getStreamAddress() {
        for (int i = 0; i < numRequest; i++) {
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            final int tokenCount = 3;
            try(Timer.Context context = MetricsUtils.getConditionalContext(getStreamSpaceTimer)) {
                rt.getSequencerView().getStreamAddressSpace(new StreamAddressRange(stream, tokenCount, Address.NON_ADDRESS));
            }
        }
    }

    @Override
    public void execute() {
        switch (shortName) {
            case "query":
                tokenQuery();
            case "raw":
                tokenRaw();
            case "multistream":
                tokenMultiStream();
            case "tx":
                tokenTx();
            case "getstreamaddr":
                getStreamAddress();
            default:
                log.error("no such operation for sequencer.");
        }
    }
}