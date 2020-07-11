package org.corfudb.universe.spec;

import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Detects file descriptor leaks in Corfu
 */
@Builder
public class FileDescriptorLeaksSpec {

    @NonNull
    private final CorfuServer server;
    @NonNull
    private final CorfuClient corfuClient;

    private final Duration defaultRestartTimeout = Duration.ofSeconds(3);

    public FileDescriptorLeaksSpec resetServer() throws Exception {
        corfuClient.getRuntime()
                .getLayoutView()
                .getRuntimeLayout()
                .getBaseClient(server.getEndpoint())
                .reset();

        TimeUnit.SECONDS.sleep(defaultRestartTimeout.getSeconds());
        return this;
    }

    public void check() {
        String[] lsofOutput = server.execute("lsof").split("\\r?\\n");
        List<LsofRecord> lsOfList = Arrays
                .stream(lsofOutput)
                .map(LsofRecord::parse)
                .collect(Collectors.toList());

        for (LsofRecord record : lsOfList) {
            if (record.path.startsWith("/app/" + server.getParams().getName() + "/db/corfu/log")) {
                fail("File descriptor leaks has been detected: " + record);
            }
        }
    }

    @ToString
    @Builder
    private static class LsofRecord {
        private final int numberOfResources;
        private final String process;
        private final String path;

        public static LsofRecord parse(String rawLsof) {
            String[] components = rawLsof.split("\t");

            return LsofRecord.builder()
                    .numberOfResources(Integer.parseInt(components[0]))
                    .process(components[1])
                    .path(components[2])
                    .build();
        }
    }
}
