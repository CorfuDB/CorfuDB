package org.corfudb.infrastructure.logreplication;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.serializer.Serializers;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class LogReplicationTest {

    String endpoint;

    private final int ITERATIONS = 10;

    CorfuTable<String, String> table1;
    CorfuTable<String, String> table2;
    CorfuTable<String, String> table3;
    CorfuRuntime runtime;

    public LogReplicationTest(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Setup Test Environment
     *
     * - Two independent Corfu Servers (source and destination)
     * - CorfuRuntime's to each Corfu Server
     *
     * @throws IOException
     */
    private void setupEnv() throws IOException {

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters.builder()
//                .trustStore("/config/cluster-manager/cluster-manager/public/truststore.jks")
//                .tsPasswordFile("/config/cluster-manager/cluster-manager/public/truststore.password")
//                .keyStore("/config/cluster-manager/cluster-manager/private/keystore.jks")
//                .ksPasswordFile("/config/cluster-manager/cluster-manager/private/keystore.password")
//                .tlsEnabled(true)
                .build();

        runtime = CorfuRuntime.fromParameters(params)
                .setTransactionLogging(true);
        runtime.parseConfigurationString(endpoint);
        runtime.registerSystemDownHandler(() -> {throw new RuntimeException("Disconnected from database. Terminating thread.");});

        System.out.println("Connecting to Corfu " + endpoint);
        runtime.connect();

        System.out.println("Connected to Corfu " + endpoint);

        //Create tables
        table1 = runtime.getObjectsView()
                .build()
                .setStreamName("Table001")
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();
        table2 = runtime.getObjectsView()
                .build()
                .setStreamName("Table002")
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setSerializer(Serializers.PRIMITIVE)
                .open();
        table3 = runtime.getObjectsView()
                .build()
                .setStreamName("Table003")
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
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
                table1.put("T1_K" + i, "T4_V" + i);
                table2.put("T2_K" + i, "T5_V" + i);
                table3.put("T3_K" + i, "T6_V" +i);
            } finally {
                runtime.getObjectsView().TXEnd();
            }
        }

    }

    private void verifyData() {
        if (table1.isEmpty() && table2.isEmpty() && table3.isEmpty()) {
            System.out.println("All tables are EMPTY");
        } else {
            System.out.println("Table1: " + table1.keySet());
            System.out.println("Table2: " + table2.keySet());
            System.out.println("Table3: " + table3.keySet());

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
            System.out.println("Generating data for the tables");
            lrTest.generateData();
            System.out.println("Generated data for the tables");
        }
        else {
            System.out.println("Verifying data for the tables");
            lrTest.verifyData();
            System.out.println("Verified data for the tables");
        }
    }
}
