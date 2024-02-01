package org.corfudb.test;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;

/**
 * Created by zlokhandwala on 2019-06-06.
 */
public class TestUtils {

    /**
     * Sets aggressive timeouts for all the router endpoints on all the runtimes.
     * <p>
     *
     * @param layout        Layout to get all server endpoints.
     * @param corfuRuntimes All runtimes whose routers' timeouts are to be set.
     */
    public static void setAggressiveTimeouts(Layout layout, CorfuRuntime... corfuRuntimes) {
        layout.getAllServers().forEach(routerEndpoint -> {
            for (CorfuRuntime runtime : corfuRuntimes) {
                runtime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            }
        });
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuRuntime corfuRuntime) {
        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    private static boolean isProcFilesystemSupported() {
        Path path = Paths.get("/proc/self/statm");
        return Files.exists(path);
    }

    public static long getRssInBytes() throws IOException, InterruptedException {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            return getRssBytesMac();
        } else if (os.contains("linux")) {
            return getRssBytesMacLinux();
        } else {
            throw new UnsupportedOperationException("Unable to get RSS using this platform.");
        }
    }

    private static long getRssBytesMac() throws IOException, InterruptedException {
        String[] command = new String[]{"sh", "-c", "ps -o rss= -p " + ProcessHandle.current().pid()};
        Process process = Runtime.getRuntime().exec(command);
        process.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = reader.readLine();
        if (line != null) {
            return Long.parseLong(line.trim()) * 1024; // Convert KB to Bytes
        } else {
            throw new IOException("Could not get RSS");
        }
    }

    private static final String STATM_PATH = "/proc/self/statm";

    public static long getRssBytesMacLinux() throws IOException, InterruptedException {
        if (!isProcFilesystemSupported()) {
            throw new UnsupportedOperationException(STATM_PATH + " not supported on this platform.");
        }
        long pageSize = getPageSize();
        long rssPages = readRssPages();
        return pageSize * rssPages;
    }

    private static long getPageSize() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("getconf", "PAGESIZE");
        Process process = pb.start();
        process.waitFor();

        if (process.exitValue() != 0) {
            throw new UnsupportedOperationException("getconf PAGESIZE not supported on this platform.");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String output = reader.readLine().trim();
            return Long.parseLong(output);
        }
    }

    private static long readRssPages() throws IOException {
        Path filePath = Paths.get(STATM_PATH);
        List<String> lines = Files.readAllLines(filePath);
        String content = lines.get(0);
        String[] parts = content.split("\\s+");
        return Long.parseLong(parts[1]); // RSS is the second value in /proc/self/statm
    }
}
