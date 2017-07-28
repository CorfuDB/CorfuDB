package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {

    public void simpleStreamTest() throws Exception {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
/*
        Runnable task = () -> {
          IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID("abc"));
          for (int x = 0; x < 1000; x++) {
              try {
                  sv.append(new byte[100]);
                  System.out.print("Write");
                  Thread.sleep(2000);
              } catch (Exception e) {
                  System.out.println("couldnt sleep");
              }
          }
        };

        Thread thread = new Thread(task);
        thread.start();
        */
        IStreamView sv = rt.getStreamsView().get(CorfuRuntime.getStreamID("txn"));
        List<ILogData> stream = sv.remainingUpTo(Long.MAX_VALUE);
        System.out.println(stream.size());
        //thread.join();
/*
        byte[] data = new byte[10*4000];
        for (long x = 0; x < 30000; x++) {
            sv.append(data);
        }
*/
    }
}
