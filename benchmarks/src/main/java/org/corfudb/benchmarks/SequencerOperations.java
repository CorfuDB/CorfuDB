package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.util.*;

@Slf4j
public class SequencerOperations extends Operation {
    SequencerOperations(String name, CorfuRuntime rt, long numRequest) {
        super(rt);
        System.out.println("sequencer operation: " + name);
        shortName = name;
        this.numRequest = numRequest;
    }

    private void tokenQuery() {
        for (int i = 0; i < numRequest; i++) {
            rt.getSequencerView().query();
        }
    }

    private void tokenRaw() {
        for (int i = 0; i < numRequest; i++) {
            rt.getSequencerView().next();
        }
    }

    private void tokenMultiStream() {
        for (int i = 0; i < numRequest; i++) {
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            rt.getSequencerView().next(stream);
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
            rt.getSequencerView().next(conflictInfo, stream);
        }
    }

    private void getStreamAddress() {
        for (int i = 0; i < numRequest; i++) {
            UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
            final int tokenCount = 3;
            rt.getSequencerView().getStreamAddressSpace(new StreamAddressRange(stream, tokenCount, Address.NON_ADDRESS));
        }
    }

    @Override
    public void execute() {
        if (shortName.equals("query")) {
            System.out.println("query");
            tokenQuery();
        } else if (shortName.equals("raw")) {
            tokenRaw();
        } else if (shortName.equals("multistream")) {
            tokenMultiStream();
        } else if (shortName.equals("tx")) {
            tokenTx();
        } else if (shortName.equals("getstreamaddr")) {
            getStreamAddress();
        } else {
            log.error("no such operation for sequencer.");
        }
    }
}
