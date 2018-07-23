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
                List<Long> range = LongStream.range(lastAddress, globalOffset + 1).boxed().collect(Collectors.toList());
                Map<Long, ILogData> map = rt.getAddressSpaceView().read(range);
                this.lastAddress = globalOffset;
                data = new ArrayList<>(map.values());
                break;
            }
        }

        return data;
    }
}
