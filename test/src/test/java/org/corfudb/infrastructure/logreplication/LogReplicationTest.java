package org.corfudb.infrastructure.logreplication;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.Serializers;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class LogReplicationTest {

    private final String endpoint;

    private final int ITERATIONS = 10;

    private PersistentCorfuTable<String, String> table1;
    private PersistentCorfuTable<String, String> table2;
    private PersistentCorfuTable<String, String> table3;
    private CorfuRuntime runtime;

    public LogReplicationTest(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Setup Test Environment
     * <p>
     * - Two independent Corfu Servers (source and destination)
     * - CorfuRuntime's to each Corfu Server
     */
    private void setupEnv() {

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters.builder()
//                .trustStore("/config/cluster-manager/cluster-manager/public/truststore.jks")
//                .tsPasswordFile("/config/cluster-manager/cluster-manager/public/truststore.password")
//                .keyStore("/config/cluster-manager/cluster-manager/private/keystore.jks")
//                .ksPasswordFile("/config/cluster-manager/cluster-manager/private/keystore.password")
//                .tlsEnabled(true)
                .build();

        runtime = CorfuRuntime.fromParameters(params);
        runtime.parseConfigurationString(endpoint);
        runtime.registerSystemDownHandler(() -> {throw new RuntimeException("Disconnected from database. Terminating thread.");});

        log.debug("Connecting to Corfu " + endpoint);
        runtime.connect();

        log.debug("Connected to Corfu " + endpoint);

        //Create tables
        table1 = runtime.getObjectsView()
                .build()
                .setStreamName("Table001")
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();
        table2 = runtime.getObjectsView()
                .build()
                .setStreamName("Table002")
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();
        table3 = runtime.getObjectsView()
                .build()
                .setStreamName("Table003")
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();
    }


    private void generateData() {

//        runtime.getObjectsView().TXBegin();
//        table1.put("SPECIAL1" , "V_PRIME");
//        runtime.getObjectsView().TXEnd();
//
//        runtime.getObjectsView().TXBegin();
//        table1.put("SPECIAL2" , "V_PRIME");
//        runtime.getObjectsView().TXEnd();


        for(int i=0; i < ITERATIONS; i++) {
            try {
                runtime.getObjectsView().TXBegin();
                table1.insert("T1_K" + i, "T4_V" + i);
                table2.insert("T2_K" + i, "T5_V" + i);
                table3.insert("T3_K" + i, "T6_V" +i);
                table3.size();
            } finally {
                runtime.getObjectsView().TXEnd();
            }
        }

    }

    private void verifyData() {
        if (table1.isEmpty() && table2.isEmpty() && table3.isEmpty()) {
            log.debug("All tables are EMPTY");
        } else {
            log.debug("Table1: " + table1.keySet());
            log.debug("Table2: " + table2.keySet());
            log.debug("Table3: " + table3.keySet());

            for (int i = 0; i < ITERATIONS; i++) {

                if (!table1.isEmpty()) assertThat(table1.get("T1_K" + i).equals("T1_V" + i));
                if (!table2.isEmpty()) assertThat(table2.get("T2_K" + i).equals("T2_V" + i));
                if (!table3.isEmpty()) assertThat(table3.get("T3_K" + i).equals("T3_V" + i));

            }
        }
    }

    public static void main(String[] args) throws Exception {
        String endpoint = args[0];
        LogReplicationTest lrTest = new LogReplicationTest(endpoint);
        lrTest.setupEnv();
        if(args[1].equalsIgnoreCase("sender")) {
            log.debug("Generating data for the tables");
            lrTest.generateData();
            log.debug("Generated data for the tables");
        }
        else {
            log.debug("Verifying data for the tables");
            lrTest.verifyData();
            log.debug("Verified data for the tables");
        }
    }
}
