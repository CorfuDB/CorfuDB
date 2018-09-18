package org.corfudb.universe.group;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.scenario.Scenario;
import org.corfudb.universe.scenario.action.AddNodeAction;
import org.corfudb.universe.scenario.fixture.Fixtures.SingleServerFixture;
import org.corfudb.universe.scenario.spec.AddNodeSpec;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.util.ClassUtils;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.Scenario.with;
import static org.corfudb.universe.scenario.fixture.Fixtures.CorfuGroupFixture;
import static org.corfudb.universe.scenario.fixture.Fixtures.MultipleServersFixture;
import static org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class DockerCorfuClusterIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();
    private static final String STREAM_NAME = "stream";

    private final DockerClient docker;
    private Universe universe;

    public DockerCorfuClusterIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    @Test
    public void testAddNode() {
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(1).build();
        CorfuGroupFixture groupFixture = CorfuGroupFixture.builder().servers(serversFixture).build();
        UniverseFixture universeFixture = UniverseFixture.builder().group(groupFixture).build();

        universe = UNIVERSE_FACTORY
                .buildDockerCluster(universeFixture.data(), docker)
                .deploy();

        SingleServerFixture node9001 = SingleServerFixture.builder()
                .mode(CorfuServer.Mode.CLUSTER)
                .port(9001)
                .build();

        CorfuCluster corfuCluster = universe.getGroup(groupFixture.getGroupName());
        Node node = corfuCluster.add(node9001.data());

        CorfuServer mainServer = corfuCluster.getNode("node9000");
        mainServer.connectCorfuRuntime();

        mainServer.addNode(ClassUtils.cast(node));
        Layout layout = mainServer.getLayout();

        assertThat(layout.getAllActiveServers().size()).isEqualTo(2);
    }

    @Test
    public void corfuServerSingleGroupSingleNodeTest() {
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(1).build();
        CorfuGroupFixture groupFixture = CorfuGroupFixture.builder().servers(serversFixture).build();
        UniverseFixture universeFixture = UniverseFixture.builder().group(groupFixture).build();

        universe = UNIVERSE_FACTORY
                .buildDockerCluster(universeFixture.data(), docker)
                .deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);
        scenario.describe((fixture, testCase) -> {
            final int size = 10;

            CorfuRuntime node1Runtime = new CorfuRuntime(serversFixture.data().get(0).getEndpoint())
                    .setCacheDisabled(true)
                    .connect();

            CorfuTable<String, String> table = node1Runtime
                    .getObjectsView()
                    .build()
                    .setType(CorfuTable.class)
                    .setStreamName(STREAM_NAME)
                    .open();

            for (int i = 0; i < size; i++) {
                table.put("key" + i, "value" + i);
            }
            int tableSize = table.size();
            assertThat(tableSize).isEqualTo(size);
        });
    }

    @Test
    public void addAndRemoveCorfuServerIntoTheClusterTest() {
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        universe = UNIVERSE_FACTORY
                .buildDockerCluster(universeFixture.data(), docker)
                .deploy();

        Scenario<UniverseParams, UniverseFixture> scenario = with(UniverseFixture.builder().build());

        scenario.describe((fixture, testCase) -> {
            UniverseParams universeParams = fixture.data();
            String groupName = universeParams.getGroups()
                    .values()
                    .asList()
                    .get(0)
                    .getName();

            CorfuCluster group = universe.getGroup(groupName);

            testCase.it("check the cluster size", Integer.class, test -> test
                    .action(() -> group.nodes().size())
                    .check((data, corfuServers) ->
                            assertThat(corfuServers).isEqualTo(fixture.getGroup().getServers().getNumNodes())
                    )
            );

            final int clusterSize = group.getParams().getNodesParams().size();
            testCase.it("should add node", Layout.class, test -> {
                        int index = 0;
                        for (Node node : group.nodes().values()) {
                            CorfuServer corfuServer = ClassUtils.cast(node);

                            if (corfuServer.getParams().getMode() == CorfuServer.Mode.SINGLE) {
                                index++;
                                continue;
                            }

                            AddNodeAction action = new AddNodeAction();
                            action.universe = universe;
                            action.setMainServerName("node9000");
                            action.setNodeName("node" + (9000 + index));
                            action.setGroupName(fixture.getGroup().getGroupName());

                            AddNodeSpec spec = new AddNodeSpec();
                            spec.setClusterSize(group.nodes().size());

                            test.action(action).check(spec);

                            index++;
                        }
                    }
            );

            testCase.it("should remove node from the cluster", Layout.class, test -> test
                    .action(() -> {
                        CorfuServer mainServer = group.getNode("node9000");
                        CorfuServer secondServer = group.getNode("node9001");
                        mainServer.removeNode(secondServer);
                        return mainServer.getLayout();
                    }).check((data, layout) -> {
                        assertThat(layout.getAllActiveServers().size()).isEqualTo(clusterSize - 1);
                    })
            );
        });
    }
}