

package org.corfudb.universe.scenario;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.group.cluster.docker.DockerCorfuCluster;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CorfuTableBenchmarkIT extends GenericIntegrationTest {

    @FunctionalInterface
    interface TriFunction<T, U, V, R> {
        R apply(T a, U b, V c);
    }

    @Test(timeout = Integer.MAX_VALUE)
    public void diskBackedTable() {
        // You might need to do: docker pull prom/prometheus
        final int metricsPort = 1234;
        final SupportClusterParams support = SupportClusterParams.builder().build();
        support.add(SupportServerParams.builder()
                .metricPorts(Collections.singleton(metricsPort))
                .nodeType(Node.NodeType.METRICS_SERVER)
                .clusterName("Metrics")
                .build());
        workflow(wf -> {
            //wf.getFixture().data().add(support);
            wf.deploy();

            CorfuCluster corfuCluster = wf.getUniverse().groups().values()
                    .stream().filter(group -> group instanceof DockerCorfuCluster)
                    .findFirst()
                    .map(group -> (CorfuCluster) group).get();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(
                    CorfuRuntime.CorfuRuntimeParameters.builder()
                            .prometheusMetricsPort(metricsPort)
                            .cacheDisabled(false));

            final int DATA_SIZE_CHAR = 10000;
            final int TX_SIZE = 100;
            final long WRITE_SIZE = 100;

            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);
            final Path persistedCacheLocation = Paths.get("/tmp/", "myData");
            ICorfuVersionPolicy.VersionPolicy versionPolicy = ICorfuVersionPolicy.MONOTONIC;

            final String tableName = "diskBackedMap";
            Map<String, String> vanillaTable = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName(tableName)
                    .open();
            Supplier<StreamingMap<String, String>> mapSupplier = () -> new PersistedStreamingMap<>(
                    persistedCacheLocation,
                    PersistedStreamingMap.getPersistedStreamingMapOptions(),
                    Serializers.JSON, corfuClient.getRuntime());
            CorfuTable<String, String>
                    rocksTable = corfuClient.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setArguments(mapSupplier, ICorfuVersionPolicy.DEFAULT)
                    .setStreamName("diskBackedMap")
                    .open();
            ICorfuTable<String, String> persistedTable = new PersistedCorfuTable<>(corfuClient.getRuntime(),
                    tableName + "Persisted");

            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();

            TriFunction<Integer, Integer, ICorfuTable<String, String>, Void> unitOfWork =
                    (offset, delay, table) -> {
                long threadId = Thread.currentThread().getId();

                Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                    try {
                        Thread.sleep((long)(Math.random() * delay));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    try (Timer.Context context = writeTimer.time()) {
                        StopWatch watch = new StopWatch();
                        watch.start();
                        corfuClient.getRuntime().getObjectsView().TXBegin();
                        for (int j = 0; j < TX_SIZE; j++) {
                            table.put(threadId + "-" + i + "-" + j, RandomStringUtils.random(DATA_SIZE_CHAR, true, true));
                        }

                        corfuClient.getRuntime().getObjectsView().TXEnd();
                        watch.stop();
                        System.err.println(i + ": "  + watch.getTime());
                    } catch (TransactionAbortedException e) {
                        e.printStackTrace();
                    }
                }

                StopWatch watch = new StopWatch();

                List<String> keys = new ArrayList<>();
                for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                    for (int j = 0; j < TX_SIZE; j++) {
                        keys.add(threadId + "-" + i + "-" + j);
                    }
                }
                Collections.shuffle(keys);
                keys.stream().findFirst().map(table::get);
                watch.reset();
                watch.start();
                corfuClient.getRuntime().getObjectsView().executeTx(() -> {
                    keys.forEach(table::get);
                });
                watch.stop();
                System.err.println("Pin Point: "  + watch.getTime());

                        AtomicInteger conut = new AtomicInteger();
                watch.reset();
                watch.start();
                corfuClient.getRuntime().getObjectsView().executeTx(() -> {
                    table.entryStream().parallel().forEach(e -> conut.incrementAndGet());
                });
                watch.stop();
                System.err.println("Scan: "  + watch.getTime());
                System.err.println("Scan: "  + conut.get());


                return null;
            };


            List<Thread> threads = new ArrayList<>();
            threads.add(new Thread(() -> unitOfWork.apply(0, 0, persistedTable)));
            //threads.add(new Thread(() -> unitOfWork.apply(0, 0, persistedTable)));
            //threads.add(new Thread(() -> unitOfWork.apply(0, 0, persistedTable)));

            threads.forEach(Thread::start);

            try {
                for (Thread thread : threads) {
                    thread.join();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            corfuClient.shutdown();
        });
    }
}

