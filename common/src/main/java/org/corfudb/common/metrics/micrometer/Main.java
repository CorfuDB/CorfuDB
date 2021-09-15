package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import org.corfudb.common.metrics.micrometer.initializers.WavefrontRegistryInitializer;

import java.time.Duration;

public class Main {
    public static void main(String[] args) {
        WavefrontRegistryInitializer init = WavefrontRegistryInitializer.builder()
                .apiToken("b6c818bb-cdb4-4739-b785-b43abcc7c968")
                .source("pavels_laptop")
                .host("10.173.65.99")
                .exportDuration(Duration.ofSeconds(5)).build();
        MeterRegistry registry = init.createRegistry();
        for(int i = 0; i < 50; i++) {
            System.out.println(i);
            registry.timer("pavel.zaytsev").record(() -> {
                try {
                    Thread.sleep(1000);
                }
                catch (Exception e) {
                    System.out.println("");
                }

            });
        }


    }
}
