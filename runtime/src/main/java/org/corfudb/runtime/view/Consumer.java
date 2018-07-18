package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class Consumer {
    public void consume(long lastAddress, CorfuRuntime rt) {
        long localOffset = -1;

        while (localOffset < lastAddress) {
            TokenResponse tr = rt.getSequencerView().query();
            long globalOffset = tr.getTokenValue();

            long delta = globalOffset - localOffset;

            if (delta == 0) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    System.out.println("Error: could not sleep!");
                }
            } else {
                for (long i = localOffset + 1; i <= globalOffset; i++) {
                    rt.getAddressSpaceView().read(i);
                }
                localOffset = globalOffset;
            }
        }
    }

    public static int main(String[] args) {
        // put driver code in here
        return 0;
    }
}
