package org.corfudb.universe;

import com.google.common.collect.ImmutableSet;
import com.spotify.docker.client.DefaultDockerClient;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.scenario.Scenario;
import org.corfudb.universe.scenario.fixture.Fixtures.AbstractUniverseFixture;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

public abstract class GenericIntegrationTest {
    private static final String POM_FILE = "pom.xml";

    @Rule
    public TestName test = new TestName();

    private UniverseManager universeManager;

    protected Universe universe;
    protected UniverseMode universeMode;

    @Before
    public void setUp() throws Exception {
        universeManager = UniverseManager.builder()
                .docker(DefaultDockerClient.fromEnv().build())
                .enableLogging(false)
                .testName(test.getMethodName())
                .universeMode(UniverseMode.VM)
                .corfuServerVersion(getAppVersion())
                .build();
        universe = universeManager.getUniverse();
        universeMode = universeManager.getUniverseMode();
    }

    @After
    public void tearDown() {
        universeManager.tearDown();
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario() {
        return universeManager.getScenario();
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario(
            int numNodes) {
        return getScenario(numNodes, ImmutableSet.of());
    }

    public Scenario<UniverseParams, AbstractUniverseFixture<UniverseParams>> getScenario(
            int numNodes, Set<Integer> metricsPorts) {
            return universeManager.getScenario(numNodes, metricsPorts);
    }

    /**
     * Provides a current version of this project. It parses the version from pom.xml
     *
     * @return maven/project version
     */
    private static String getAppVersion() {
        String version = System.getProperty("project.version");
        if (version != null && !version.isEmpty()) {
            return version;
        }

        return parseAppVersionInPom();
    }

    private static String parseAppVersionInPom() {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model;
        try {
            model = reader.read(new FileReader(POM_FILE));
            return model.getParent().getVersion();
        } catch (IOException | XmlPullParserException e) {
            throw new NodeException("Can't parse application version", e);
        }
    }
}
