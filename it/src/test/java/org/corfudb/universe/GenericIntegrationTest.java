package org.corfudb.universe;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.FileReader;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Common parent class for all universe tests.
 * Each test should extend GenericIntegrationTest class.
 * The class provides the initialization steps, it creates UniverseManager that is used to provide
 * a universe framework workflow
 * to manage your initialization and deployment process in the universe.
 */
public abstract class GenericIntegrationTest {
    private static final AppUtil APP_UTIL = new AppUtil();

    @Rule
    public TestName test = new TestName();

    private UniverseManager universeManager;

    @Before
    public void setUp() {
        universeManager = UniverseManager.builder()
                .testName(test.getMethodName())
                .universeMode(UniverseMode.PROCESS)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
    }

    public <T extends Fixture<UniverseParams>> UniverseWorkflow workflow(
            Consumer<UniverseWorkflow<T>> action) {

        return universeManager.workflow(action);
    }

    private static class AppUtil {
        private static final String POM_FILE = "pom.xml";

        /**
         * Provides a current version of this project. It parses the version from pom.xml
         *
         * @return maven/project version
         */
        public String getAppVersion() {
            String version = System.getProperty("project.version");
            if (version != null && !version.isEmpty()) {
                return version;
            }

            return parseAppVersionInPom();
        }

        public String parseAppVersionInPom() {
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
}
