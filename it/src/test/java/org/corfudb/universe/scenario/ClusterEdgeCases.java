package org.corfudb.universe.scenario;

import org.assertj.core.api.Assertions;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A set of tests that deal with edge-case reconfiguration scenarios.
 */
public class ClusterEdgeCases extends GenericIntegrationTest {
    /**
     * The test will forcefully bump up one of the LayoutServer epochs, thus emulating
     * a partial seal. The cluster should recover and all nodes should reach the same layout.
     */
    @Test(timeout = 300000)
    public void wrongEpochRecovery() {
        getScenario().describe((fixture, testCase) -> {
            final CorfuCluster corfuCluster =
                    universe.getGroup(fixture.getCorfuCluster().getName());
            final CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            testCase.it("Should fail one link and then heal", data -> {
                final Layout originalLayout = corfuClient.getRuntime().getLayoutView().getLayout();
                final RuntimeLayout runtimeLayout = corfuClient.getRuntime()
                        .getLayoutView()
                        .getRuntimeLayout(originalLayout);

                // Forcefully bump up the epoch.
                runtimeLayout.getBaseClient(
                        originalLayout.getLayoutServers().stream().findFirst().get()
                ).sealRemoteServer(originalLayout.getEpoch() + 10);

                {
                    final List<Throwable> exceptions = new ArrayList();
                    final List<Layout> layouts = new ArrayList();

                    Stream<CompletableFuture<Layout>> layoutFutures = originalLayout
                            .getLayoutServers().stream()
                            .map(server -> runtimeLayout.getLayoutClient(server).getLayout());

                    // Make sure we actually get the WrongEpochException.
                    layoutFutures.forEach(future -> {
                        try {
                            layouts.add(future.get());
                        } catch (InterruptedException e) {
                            Assertions.fail("Test interrupted.");
                        } catch (ExecutionException e) {
                            exceptions.add(e.getCause());
                        }
                    });

                    Assertions.assertThat(layouts.size()).isEqualTo(1);
                    Assertions.assertThat(exceptions.size())
                            .isEqualTo(originalLayout.getLayoutServers().size()- 1);
                    exceptions.forEach(ex -> Assert.assertTrue(ex instanceof WrongEpochException));
                }

                // Wait for the cluster to heal and stabilize.
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        Assertions.fail("Test interrupted.");
                    }

                    Stream<CompletableFuture<Layout>> layoutFutures = originalLayout
                            .getLayoutServers().stream()
                            .map(server -> runtimeLayout.getLayoutClient(server).getLayout());

                    final List<Layout> layouts = new ArrayList();

                    layoutFutures.forEach(future -> {
                        try {
                            layouts.add(future.get());
                        } catch (InterruptedException e) {
                            Assertions.fail("Test interrupted.");
                        } catch (ExecutionException ignored) {
                        }
                    });

                    if (layouts.size() == originalLayout.getLayoutServers().size()) {
                        break; // The cluster was able to recover from the partial seal.
                    }
                }
            });

            corfuClient.shutdown();
        });
    }
}
