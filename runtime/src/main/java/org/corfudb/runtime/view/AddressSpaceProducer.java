package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class AddressSpaceProducer implements Producer {
    final CorfuRuntime runtime;

     public AddressSpaceProducer(CorfuRuntime rt) {
         runtime = rt;
     }

     public long send(Object payload) {
         while(true) {
             try {
                 int numAsync = 100;

                 // acquire tokens
                 final SequencerClient client = new RuntimeLayout(runtime.getLayoutView().getLayout(),
                         runtime).getPrimarySequencerClient();
                 CompletableFuture[] futures = new CompletableFuture[numAsync];
                 TokenResponse[] trs = new TokenResponse[numAsync];
                 for (int t = 0; t < numAsync; t++) {
                     futures[t] = client.nextToken(Collections.EMPTY_LIST, 1);
                 }
                 for (int t = 0; t < numAsync; t++) {
                     try {
                         trs[t] = (TokenResponse) futures[t].get();
                     } catch (Exception e) {
                         System.out.println(e);
                         throw new RuntimeException(e);
                     }
                 }

                 // perform writes
                 final RuntimeLayout runtimeLayout = new RuntimeLayout(runtime.getLayoutView().getLayout(), runtime);
                 final ILogData ld = new LogData(DataType.DATA, payload);
                 for (int i = 0; i < numAsync; i++) {
                     runtimeLayout.getLogUnitClient(trs[i].getTokenValue(), 0).write(ld);
                 }

                 return 0;
             } catch (Exception e) {
                 System.out.println(e);
                 continue;
             }
         }
     }
}
