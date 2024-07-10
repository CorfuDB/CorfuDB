package org.corfudb.infrastructure;

import io.netty.channel.EventLoopGroup;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.common.config.ConfigParamNames.DISABLE_FILE_WATCHER;
import static org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig.DEFAULT_DISABLE_FILE_WATCHER;

/**
 * Created by cgudisagar on 1/25/24.
 */
public class ServerContextTest {
    /**
     * Test that {@link ServerContext#refreshWorkerGroupThreads()}# stops the old worker group threads
     * and spawn new ones.
     */
    @Test
    public void refreshWorkerGroupThreadsTest() {
        final int port = 9000;
        try (ServerContext context = new ServerContextBuilder()
                .setPort(port)
                .build()) {

            // Asserting the default value of disableFileWatcher
            assertThat(
                    context.getServerConfig().get(DISABLE_FILE_WATCHER)
            ).isEqualTo(DEFAULT_DISABLE_FILE_WATCHER);

            EventLoopGroup workersGroup;
            EventLoopGroup newWorkerGroup;
            workersGroup = context.getWorkerGroup();

            // Refresh workerGroup threads
            context.refreshWorkerGroupThreads();

            newWorkerGroup = context.getWorkerGroup();
            assertThat(workersGroup).isNotEqualTo(newWorkerGroup);

            // Old workersGroup should be terminated
            assertThat(workersGroup.isShuttingDown()).isTrue();
            assertThat(workersGroup.isShutdown()).isTrue();
            assertThat(workersGroup.isTerminated()).isTrue();

            // New workers group should not be terminated
            assertThat(newWorkerGroup.isShuttingDown()).isFalse();
            assertThat(newWorkerGroup.isShutdown()).isFalse();
            assertThat(newWorkerGroup.isTerminated()).isFalse();
        }
    }
}
