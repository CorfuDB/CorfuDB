package org.corfudb.integration.pg;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.LogReplicationAbstractIT;

@Slf4j
public class PostgresConfigTest extends LogReplicationAbstractIT {
    private static final String TEST_PLUGIN_CONFIG_PATH_ACTIVE =
            "./test/src/test/resources/transport/postgresConfigActive.properties";

    private static final String TEST_PLUGIN_CONFIG_PATH_STANDBY =
            "./test/src/test/resources/transport/postgresConfigStandby.properties";

    // TODO (Postgres): need to implement way for test to bring up own postgres cluster
    // @Test
    public void testReplication() throws Exception {
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        Thread.sleep(60000);
    }
}
