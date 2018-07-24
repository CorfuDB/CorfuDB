package org.corfudb.runtime.view;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

public class AddressSpaceConsumer implements Consumer {
    final CorfuRuntime rt;
    long lastAddress = -1;
    long tempDelta = 0;

    public AddressSpaceConsumer(CorfuRuntime runtime) {
        rt = runtime;
    }

    @Override
    public List<ILogData> poll(long timeout) {
        List<ILogData> data = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime <= timeout) {
            long globalOffset = rt.getSequencerView().query().getTokenValue();
            long delta = globalOffset - lastAddress;

            if (delta != 0) {
                for (long i = lastAddress + 1; i <= globalOffset; i++) {
                    ILogData ld = rt.getAddressSpaceView().read(i);
                    data.add(ld);
                }
                this.lastAddress = globalOffset;
                break;
            }
        }
        return data;
    }
}
