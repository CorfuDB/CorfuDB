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
    SequencerOperations(String name, CorfuRuntime rt) {
        super(rt);
        shortName = name;
    }

    private void token_query() {
        rt.getSequencerView().query();
    }

    private void token_raw() {
        rt.getSequencerView().next();
    }

    private void token_multi_stream() {
        UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
        rt.getSequencerView().next(stream);
    }

    private void token_tx() {
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

    private void getStreamAddress() {
        UUID stream = UUID.nameUUIDFromBytes("Stream".getBytes());
        final int tokenCount = 3;
        rt.getSequencerView().getStreamAddressSpace(new StreamAddressRange(stream,  tokenCount, Address.NON_ADDRESS));
    }

    @Override
    public void execute() {
        if (shortName.equals("query")) {
            token_query();
        } else if (shortName.equals("raw")) {
            token_raw();
        } else if (shortName.equals("multistream")) {
            token_multi_stream();
        } else if (shortName.equals("tx")) {
            token_tx();
        } else if (shortName.equals("getstreamaddr")) {
            getStreamAddress();
        } else {
            log.error("no such operation for sequencer.");
        }
    }
}
