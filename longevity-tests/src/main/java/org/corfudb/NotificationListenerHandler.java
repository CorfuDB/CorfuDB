package org.corfudb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.*;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class NotificationListenerHandler extends StreamListenerResumeOrFullSync {
    CommonUtils commonUtils;
    ExecutorService executorService;
    NotificationListenerHandler(String name, CorfuStore corfuStore, CommonUtils commonUtils, String namespace, String streamtag) {
        super(corfuStore, namespace, streamtag);
        this.commonUtils = commonUtils;
        this.executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(name)
                        .build());
    }

    @Override
    public void onNextEntry(CorfuStreamEntries results) {
        long currentTime = System.currentTimeMillis();
        executorService.submit(() ->
            results.getEntries().forEach((schema, entries) ->
            {
                onEachSchema(schema, entries);
                entries.forEach(entry -> {
                    onEachEntry(schema, entry);
                    ExampleSchemas.ManagedMetadata metadata = (ExampleSchemas.ManagedMetadata) entry.getMetadata();
                    MicroMeterUtils.time(Duration.ofMillis(currentTime - metadata.getLastModifiedTime()),
                            "notification.receive.timer", "single");
                });
            }
        ));
    }

    abstract void onEachSchema(TableSchema tableSchema, List<CorfuStreamEntry> entry);
    abstract void onEachEntry(TableSchema tableSchema, CorfuStreamEntry entry);

    public void shutdown() {
        executorService.shutdownNow();
    }
}
