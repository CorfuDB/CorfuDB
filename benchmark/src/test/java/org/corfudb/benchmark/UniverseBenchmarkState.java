package org.corfudb.benchmark;

import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class UniverseBenchmarkState {
    public CorfuCluster corfuCluster;
    public UniverseManager universeManager;
    public CorfuClient corfuClient;
    public UniverseManager.UniverseWorkflow<Fixture<UniverseParams>> wf;
}
