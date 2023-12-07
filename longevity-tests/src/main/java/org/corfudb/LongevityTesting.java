package org.corfudb;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.CommonUtils.Config;
import org.corfudb.util.NodeLocator;

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

    public static void main(String[] args) {
        Config config = new Config();
        JCommander jCommander = new JCommander(config);
        jCommander.parse(args);
        LongevityTesting longevityTesting = new LongevityTesting();
        try {
            longevityTesting.startTestMain(config);
        } catch (Exception e) {
            log.error("Exception encountered: ", e);
        }
    }

    public void startTestMain(Config config) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        CorfuRuntime runtime = buildRuntime(config);
        CorfuStore corfuStore = new CorfuStore(runtime);
        CommonUtils commonUtils = new CommonUtils(corfuStore);

        log.info("Welcome");
        List<Workflow> workflows = new ArrayList<>();

        for (int i=0; i < config.listWorkflows.size(); i++) {
            Class<?> workflowClass = Class.forName(config.listWorkflows.get(i));
            Constructor<?> workflowConstructor = workflowClass.getConstructor(String.class, String.class);
            Workflow w = (Workflow) workflowConstructor.newInstance(config.listWorkflows.get(i),
                    config.listWorkflowPropFiles.get(i));
            w.init(runtime, commonUtils);
            w.start();
            workflows.add(w);
        }


        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(config.configFile))) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<NotificationListenerHandler> notificationListeners = new ArrayList<>();
        List<String> listenerClassNames = Arrays.asList(properties.getProperty("listener.handlers").split(","));
        List<String> listenerPropFiles = Arrays.asList(properties.getProperty("listener.properties.files").split(","));
        List<String> namespaces = Arrays.asList(properties.getProperty("namespaces").split(","));
        List<String> streamTags = Arrays.asList(properties.getProperty("streamTags").split(","));

        for (int i=0; i<listenerClassNames.size(); i++) {
            log.info("Invoking listeners...");
            Class<?> listenerClass = Class.forName(listenerClassNames.get(i));
            NotificationListenerHandler l;
            if (listenerPropFiles.get(i).contains("null")) {
                Constructor<?> listenerConstructor = listenerClass.getConstructor(String.class, CorfuStore.class, CommonUtils.class,
                        String.class, String.class);
                l = (NotificationListenerHandler) listenerConstructor.newInstance(
                        listenerClassNames.get(i), corfuStore, commonUtils, namespaces.get(i), streamTags.get(i));
            } else {
                Constructor<?> listenerConstructor = listenerClass.getConstructor(String.class, CorfuStore.class, CommonUtils.class,
                        String.class, String.class, String.class);
                l = (NotificationListenerHandler) listenerConstructor.newInstance(listenerClassNames.get(i),
                        corfuStore, commonUtils, namespaces.get(i), streamTags.get(i), listenerPropFiles.get(i));
            }
            corfuStore.subscribeListener(l, namespaces.get(i), streamTags.get(i));
            notificationListeners.add(l);
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

    public CorfuRuntime buildRuntime(Config config) {
        NodeLocator nodeLocator = NodeLocator.builder().host(config.host).port(Integer.parseInt(config.port)).build();
        CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder();
        builder.tlsEnabled(config.tlsEnabled);
        if (config.tlsEnabled) {
            builder.keyStore(config.keystore);
            builder.ksPasswordFile(config.ks_password);
            builder.trustStore(config.truststore);
            builder.tsPasswordFile(config.truststore_password);
        }
        return CorfuRuntime.fromParameters(builder.build()).parseConfigurationString(
                nodeLocator.toEndpointUrl()).connect();
    }
}