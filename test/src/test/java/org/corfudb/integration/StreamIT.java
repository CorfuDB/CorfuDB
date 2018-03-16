package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {

    @Test
    public void simpleStreamTest() throws Exception {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();


        Map<String, String> map = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();
        Map<String, String> map2 = rt.getObjectsView().build().setStreamName("s2").setType(SMRMap.class).open();


        final int iter1 = 300;
        for (int x = 0; x < iter1; x++) {
            if (x % 2 == 0) {
                rt.getObjectsView().TXBegin();
                map.put(String.valueOf(x),
                        String.valueOf(x));
                rt.getObjectsView().TXEnd();
            } else {
                rt.getObjectsView().TXBegin();
                map2.put(String.valueOf(x),
                        String.valueOf(x));
                rt.getObjectsView().TXEnd();
            }

        }

        //System.out.println("Seeking on " + CorfuRuntime.getStreamID("s1"));
        //rt.getStreamsView().get(CorfuRuntime.getStreamID("s1")).seek(0);
        //rt.getStreamsView().get(CorfuRuntime.getStreamID("s1")).nextUpTo(400);


        System.out.println("-----------------Reading------------------");

        final int iter2 = 100;
        final int diff = 3;
        for (int x = 0; x < iter2; x++) {
            rt.getObjectsView().TXBuild().setSnapshot(Math.min((long) Math.pow(-1, x) * diff, 0) + x).setType(TransactionType.SNAPSHOT).begin();
            //rt.getObjectsView().TXBuild().setSnapshot(Math.min(((long) Math.pow(-1, x) * 3  + x), 0)).setType(TransactionType.SNAPSHOT).begin();
            System.out.println(Math.min((long) Math.pow(-1, x) * diff, 0) + x);



            map.get("key1");
            rt.getObjectsView().TXEnd();
        }


        System.out.println("-----------------Verifying------------------");

        //rt.getObjectsView().TXBuild().setSnapshot(5).setType(TransactionType.SNAPSHOT).begin();

        //assertThat(map.get(String.valueOf(0))).isEqualTo(String.valueOf(0));
        //assertThat(map.get(String.valueOf(2))).isEqualTo(String.valueOf(2));
        //assertThat(map.get(String.valueOf(4))).isEqualTo(String.valueOf(4));
        //assertThat(map).hasSize(3);

        //rt.getObjectsView().TXEnd();

        final int snapshotUpperLimit = 5;

        for (int snapshotVersion = 1; snapshotVersion <= snapshotUpperLimit; snapshotVersion++) {
            rt.getObjectsView().TXBuild().setSnapshot(snapshotVersion).setType(TransactionType.SNAPSHOT).begin();
            for (int o = 1; o <= snapshotVersion; o = o + 2) {
                System.out.println("version " + o);
                assertThat(map2.get(String.valueOf(o))).isEqualTo(String.valueOf(o));
            }

            for (int e = 0; e <= snapshotVersion; e = e + 2) {
                System.out.println("version " + e);
                assertThat(map.get(String.valueOf(e))).isEqualTo(String.valueOf(e));
            }

            assertThat(map.size() + map2.size()).isEqualTo(snapshotVersion + 1);
            rt.getObjectsView().TXEnd();
        }
        //assertThat(map2).hasSize(3);



        // Verify data
        /**
        System.out.println("-----------------Verifying------------------");
        for (int x = 5; x < iter1; x++) {
            System.out.println("snapshot " + x);
            rt.getObjectsView().TXBuild().setSnapshot(x).setType(TransactionType.SNAPSHOT).begin();

            for (int e = 0; e <= x; e = e * 2) {
                assertThat(map.get(String.valueOf(e))).isEqualTo(String.valueOf(e));
            }

            for (int o = 0; o <= x; o = (o * 2) + 1) {
                assertThat(map.get(String.valueOf(o))).isEqualTo(String.valueOf(o));
            }

            rt.getObjectsView().TXEnd();
        }
         **/

    }
}
