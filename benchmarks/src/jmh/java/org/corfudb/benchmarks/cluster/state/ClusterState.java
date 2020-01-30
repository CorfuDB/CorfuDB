package org.corfudb.benchmarks.cluster.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.benchmarks.util.DataGenerator;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.UniverseAppUtil;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.universe.Universe;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

@Slf4j
public abstract class ClusterState {
    public final Random rnd = new Random();

    private UniverseWorkflow workflow;

    public final List<CorfuClient> corfuClients = new ArrayList<>();
    public final List<CorfuTable<String, String>> tables = new ArrayList<>();
    public final AtomicInteger counter = new AtomicInteger(1);

    private String getAppVersion() {
        return new UniverseAppUtil().getAppVersion();
    }

    public CorfuClient getRandomCorfuClient() {
        return corfuClients.get(rnd.nextInt(corfuClients.size()));
    }

    public CorfuTable<String, String> getRandomTable() {
        return tables.get(rnd.nextInt(tables.size()));
    }

    protected void fillTables(int numEntries, String data) {
        tables.forEach(table -> {
            for (int i = 0; i < numEntries; i++) {
                table.put(String.valueOf(i), data);
            }
        });
    }

    public void init(int numServers, int numRuntime, int numTables) {

        UniverseManager universeManager = UniverseManager.builder()
                .testName("corfu_cluster_benchmark")
                .universeMode(Universe.UniverseMode.PROCESS)
                .corfuServerVersion(getAppVersion())
                .build();

        workflow = universeManager.workflow(wf -> {
            wf.setupProcess(fixture -> {
                fixture.getCluster().numNodes(numServers);
                fixture.getServer().serverJarDirectory(Paths.get("benchmarks", "target"));

                //disable automatic shutdown
                fixture.getUniverse().cleanUpEnabled(false);
            });

            wf.setupDocker(fixture -> {
                fixture.getCluster().numNodes(numServers);
                fixture.getServer().serverJarDirectory(Paths.get("benchmarks", "target"));

                //disable automatic shutdown
                fixture.getUniverse().cleanUpEnabled(false);
            });

            wf.setupVm(fixture -> {
                fixture.setVmPrefix("corfu-vm-dynamic-qe");
                fixture.getCluster().name("static_cluster");
            });

            wf.deploy();

            CorfuCluster corfuCluster = wf
                    .getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            for (int i = 0; i < numRuntime; i++) {
                corfuClients.add(corfuCluster.getLocalCorfuClient());
            }

            for (int i = 0; i < numTables; i++) {
                CorfuClient corfuClient = getRandomCorfuClient();
                CorfuTable<String, String> table = corfuClient
                        .createDefaultCorfuTable(DEFAULT_STREAM_NAME + i);
                tables.add(table);
            }
        });
    }

    public void tearDown() {
        tables.forEach(CorfuTable::close);
        corfuClients.forEach(CorfuClient::shutdown);
        workflow.shutdown();
    }

    @State(Scope.Benchmark)
    @Slf4j
    @Getter
    public static class ClusterStateForRead extends ClusterState {

        @Param({})
        public int numServers;

        @Param({})
        public int numRuntime;

        @Param({})
        public int numTables;

        @Param({})
        public int dataSize;

        @Param({})
        public int numEntries;

        private String data = DataGenerator.generateDataString(dataSize);

        @Setup
        public void init() {
            super.init(numServers, numRuntime, numTables);
            fillTables(numEntries, data);
        }

        public int getRandomIndex() {
            return rnd.nextInt(numEntries);
        }
    }

    @State(Scope.Benchmark)
    @Slf4j
    @Getter
    public static class ClusterStateForWrite extends ClusterState {

        @Param({})
        public int numServers;

        @Param({})
        public int numRuntime;

        @Param({})
        public int numTables;

        @Param({})
        public int dataSize;

        @Param({})
        public int numEntries;

        @Param({})
        public int txSize;

        private String data = DataGenerator.generateDataString(dataSize);

        @Setup
        public void init() {
            super.init(numServers, numRuntime, numTables);
        }

        public void txBegin() {
            corfuClients.get(0).getObjectsView().TXBegin();
        }

        public void txEnd() {
            corfuClients.get(0).getObjectsView().TXEnd();
        }
    }
}
