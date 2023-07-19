package org.corfudb;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.CommonUtils.Config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LongevityTesting {

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        Config config = new Config();
        JCommander jCommander = new JCommander(config);
        jCommander.parse(args);
        CorfuRuntime runtime = new CorfuRuntime(config.serverEndpoints).connect();
        CorfuStore corfuStore = new CorfuStore(runtime);
        CommonUtils commonUtils = new CommonUtils(corfuStore);

        log.info("Welcome");
        List<Workflow> workflows = new ArrayList<>();

        for (int i=0; i < config.listWorkflows.size(); i++) {
            Class<?> workflowClass = Class.forName(config.listWorkflows.get(i));
            Constructor<?> workflowConstructor = workflowClass.getConstructor(String.class);
            Workflow w = (Workflow) workflowConstructor.newInstance(config.listWorkflows.get(i));
            w.init(config.listWorkflowPropFiles.get(i), runtime, commonUtils);
            w.start();
            workflows.add(w);
        }

        List<NotificationListenerHandler> notificationListeners = new ArrayList<>();

        try (InputStream input = Files.newInputStream(Paths.get(config.configFile))) {
            Properties properties = new Properties();
            properties.load(input);
            List<String> listenerClassNames = Arrays.asList(properties.getProperty("listener.handlers").split(","));
            List<String> namespaces = Arrays.asList(properties.getProperty("namespaces").split(","));
            List<String> streamTags = Arrays.asList(properties.getProperty("streamTags").split(","));

            for (int i=0; i<listenerClassNames.size(); i++) {
                Class<?> listenerClass = Class.forName(listenerClassNames.get(i));
                Constructor<?> listenerConstructor = listenerClass.getConstructor(String.class, CorfuStore.class, CommonUtils.class,
                        String.class, String.class);
                NotificationListenerHandler l = (NotificationListenerHandler) listenerConstructor.newInstance(
                        listenerClassNames.get(i), corfuStore, commonUtils, namespaces.get(i), streamTags.get(i));
                corfuStore.subscribeListener(l, namespaces.get(i), streamTags.get(i));
                notificationListeners.add(l);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            TimeUnit.MINUTES.sleep(config.testDuration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        log.info("Stopping all");
        for (Workflow w : workflows) {
            w.stop();
        }
        for(NotificationListenerHandler l : notificationListeners) {
            corfuStore.unsubscribeListener(l);
            l.shutdown();
        }
    }
}