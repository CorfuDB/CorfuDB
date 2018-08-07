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

     public long send(Object payload, int numAsync) {
         while(true) {
             try {
                 final RuntimeLayout runtimeLayout = new RuntimeLayout(runtime.getLayoutView().getLayout(), runtime);

                 // acquire tokens
                 final SequencerClient client = runtimeLayout.getPrimarySequencerClient();
                 CompletableFuture[] futures = new CompletableFuture[numAsync];
                 TokenResponse[] trs = new TokenResponse[numAsync];
                 for (int t = 0; t < numAsync; t++) {
                     futures[t] = client.nextToken(Collections.EMPTY_LIST, 1);
                 }
                 for (int t = 0; t < numAsync; t++) {
                     try {
                         trs[t] = (TokenResponse) futures[t].get();
                     } catch (Exception e) {
                         throw new RuntimeException(e);
                     }
                 }

                 // perform writes
                 long globalAddress = -1;
                 CompletableFuture[] writeFutures = new CompletableFuture[numAsync];
                 for (int i = 0; i < numAsync; i++) {
                     ILogData ld = new LogData(DataType.DATA, payload);
                     ld.useToken(trs[i].getToken());
                     ld.setId(runtime.getParameters().getClientId());
                     globalAddress = ld.getGlobalAddress();
                     //ILogData.SerializationHandle sh = ld.getSerializedForm();
                     writeFutures[i] = runtimeLayout.getLogUnitClient(globalAddress, 0).write(ld); //.write(sh.getSerialized());
                 }
                 for (int t = 0; t < numAsync; t++) {
                     try {
                         writeFutures[t].get();
                     } catch (Exception e) {
                         throw new RuntimeException(e);
                     }
                 }

                 return 0;
             } catch (Exception e) {
                 System.out.println(e);
                 continue;
             }
         }
     }
}
