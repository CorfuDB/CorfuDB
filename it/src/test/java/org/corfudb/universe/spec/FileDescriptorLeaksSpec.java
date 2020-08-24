package org.corfudb.universe.spec;

import lombok.Builder;
import lombok.Builder.Default;
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

    public FileDescriptorLeaksSpec resetServer() {
        corfuClient.getRuntime()
                .getLayoutView()
                .getRuntimeLayout()
                .getBaseClient(server.getEndpoint())
                .reset();

        return this;
    }

    public FileDescriptorLeaksSpec timeout() throws InterruptedException {
        return timeout(defaultRestartTimeout);
    }

    public FileDescriptorLeaksSpec timeout(Duration duration) throws InterruptedException {
        TimeUnit.SECONDS.sleep(duration.getSeconds());
        return this;
    }

    public void check() {
        String[] lsofOutput = server.execute("lsof").split("\\r?\\n");
        List<LsofRecord> lsOfList = Arrays
                .stream(lsofOutput)
                .map(LsofRecord::parse)
                .collect(Collectors.toList());

        for (LsofRecord record : lsOfList) {
            boolean isDeleted = record.state == FileState.DELETED;
            boolean isCorfuLogFile = record.path.startsWith("/app/" + server.getParams().getName() + "/db/corfu/log");

            if (isCorfuLogFile && isDeleted) {
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
        @Default
        private final FileState state = FileState.NA;

        public static LsofRecord parse(String rawLsof) {
            String[] components = rawLsof.split("\t");

            String path = components[2];
            String[] pathAndState = path.split(" ");

            FileState state = FileState.OPEN;
            if (pathAndState.length > 1) {
                String stateStr = pathAndState[pathAndState.length - 1].trim();
                if (stateStr.equals("(deleted)")){
                    state = FileState.DELETED;
                }
            }

            return LsofRecord.builder()
                    .numberOfResources(Integer.parseInt(components[0]))
                    .process(components[1])
                    .path(path)
                    .state(state)
                    .build();
        }
    }

    enum FileState {
        DELETED, OPEN, NA
    }
}