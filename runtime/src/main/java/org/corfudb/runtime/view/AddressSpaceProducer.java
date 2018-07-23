package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class AddressSpaceProducer implements Producer {
    final CorfuRuntime runtime;

     public AddressSpaceProducer(CorfuRuntime rt) {
         runtime = rt;
     }

     public long send(Object payload) {
         while(true) {
             try {
                 TokenResponse tr = runtime.getSequencerView().next();
                 Token t = tr.getToken();
                 runtime.getAddressSpaceView().write(t, payload);
                 return t.getTokenValue(); // return address at which payload was placed
             } catch (Exception e) {
                 continue;
             }
         }
     }
}
