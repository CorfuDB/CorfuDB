package org.corfudb.infrastructure.logreplication.infrastructure;

import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuInterClusterReplicationServerTest {
    private final static String REPLICATION_DIR = "/tmp/replication";

    @Test
    public void testCreateReplicationDirectory() {
        Map<String, Object> opts = new HashMap<>();

        opts.put("--memory", true);
        opts.put("--log-path", REPLICATION_DIR);
        CorfuInterClusterReplicationServer.createReplicationDirectory(opts);
        File file = new File((String) opts.get("--log-path"));
        assertThat(file).doesNotExist();

        opts.put("--memory", false);
        CorfuInterClusterReplicationServer.createReplicationDirectory(opts);
        file = new File((String) opts.get("--log-path"));
        assertThat(file).isDirectory();

        assertThat(file.delete()).isTrue();
    }
}
