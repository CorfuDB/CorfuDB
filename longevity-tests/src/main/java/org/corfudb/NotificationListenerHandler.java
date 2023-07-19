package org.corfudb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.runtime.collections.*;

import java.lang.reflect.InvocationTargetException;
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
        executorService.submit(() -> onNext(results));
    }

    public void shutdown() {
        executorService.shutdownNow();
    }
}
