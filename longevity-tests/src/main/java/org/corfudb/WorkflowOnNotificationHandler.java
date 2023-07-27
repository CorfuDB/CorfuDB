package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WorkflowOnNotificationHandler extends NotificationListenerHandler {

    Properties properties;
    Workflow workflow;
    public WorkflowOnNotificationHandler(String name, CorfuStore corfuStore,
                                  CommonUtils commonUtils,
                                  String namespace, String streamtag, String propertiesFilePath) {
        super(name, corfuStore, commonUtils, namespace, streamtag);
        log.info("Initializing WorkflowOnNotificationHandler...");
        try (InputStream input = Files.newInputStream(Paths.get(propertiesFilePath))) {
            properties = new Properties();
            properties.load(input);

            String workflowName = properties.getProperty("workflow.name");
            Class<?> workflowClass = Class.forName(workflowName);
            Constructor<?> workflowConstructor = workflowClass.getConstructor(String.class);
            workflow = (Workflow) workflowConstructor.newInstance(workflowName);
            workflow.init(properties.getProperty("workflow.properties.file"), corfuStore.getRuntime(), commonUtils);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onNext(CorfuStreamEntries results) {

        workflow.start();
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        workflow.stop();
    }

    @Override
    protected CorfuStoreMetadata.Timestamp performFullSync() {
        return null;
    }
}
