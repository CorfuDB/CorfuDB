package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class Producer {
    public long produce(int numReq, Object payload, CorfuRuntime rt) { // look ar Kafka api for names or queue

        // remove numReq cuz just once
        // rt = move to constructor

        long time = 0;

        for (int i = 0; i < numReq; i++) {
            long ts1 = System.currentTimeMillis();

            // put in a loop that retries until it works
            TokenResponse tr = rt.getSequencerView().next();
            Token t = tr.getToken();
            rt.getAddressSpaceView().write(t, payload);

            long ts2 = System.currentTimeMillis();
            time += (ts2 - ts1);
        }

        System.out.println("(Producer) Latency [ms/op]: " + ((time*1.0)/numReq));
        System.out.println("(Producer) Throughput [ops/ms]: " + (numReq/(time*1.0)));

        return rt.getSequencerView().next().getTokenValue(); // last address
    }
}
