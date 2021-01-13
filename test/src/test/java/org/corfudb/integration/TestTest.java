package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;

public class TestTest {

    @Test
    public void test() {
        String first = "localhost:9000";
        String second = "localhost:9001";
        String third = "localhost:9002";
        String stream = "test";
        CorfuRuntime rt = new CorfuRuntime(first)
                .setCacheDisabled(true)
                .connect();
//        CorfuTable<String, String> map = rt.getObjectsView()
//                .build()
//                .setStreamName(stream)
//                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
//                })
//                .option(ObjectOpenOption.NO_CACHE)
//                .open();
//
//        System.out.println("Writing");
//        for (int i = 0; i < 100; i++) {
//            rt.getObjectsView().TXBegin();
//            map.put("key" + i, "value" + i);
//            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
//            System.out.println(i);
//            rt.getObjectsView().TXEnd();
//        }
//
//        System.out.println("Reading");
//        for (int i = 0; i < 100; i++) {
//            map.get("key" + i);
//            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
//        }
        System.out.println("Adding second");
        rt.getManagementView().addNode(second, 3, Duration.ofSeconds(3), Duration.ofSeconds(5));

        while (true) {
            rt.invalidateLayout();
            Layout layout = rt.getLayoutView().getLayout();
            if (layout.getAllLogServers().size() == 2) {
                break;
            }
        }
//        System.out.println("Adding more data");
//        for (int i = 100; i < 200; i++) {
//            rt.getObjectsView().TXBegin();
//            map.put("key" + i, "value" + i);
//            Sleep.sleepUninterruptibly(Duration.ofSeconds(1));
//            rt.getObjectsView().TXEnd();
//        }

        System.out.println("Adding third");
        rt.getManagementView().addNode(third, 3, Duration.ofSeconds(3), Duration.ofSeconds(5));

        while (true) {
            rt.invalidateLayout();
            Layout layout = rt.getLayoutView().getLayout();
            if (layout.getAllLogServers().size() == 3) {
                break;
            }
        }
    }
}
