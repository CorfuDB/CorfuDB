package org.corfudb.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.FileUtils;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.JsonUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Parameters(commandDescription = "Bootstrap cluster")
public class BootstrapCommand extends AuthenticationArgs {

    @Parameter(names = "--layout", description = "Path to the json layout file.", required = true)
    private String layoutFile;

    @Parameter(names = "--retry", description = "Number of times to retry on failure.")
    private int numRetry = 3;

    @Parameter(names = "-retry-duration", description = "Duration in seconds to wait between retries.")
    private int retryDuration = 5;

    private Layout parseLayout(String layoutPath) throws IOException {
        File file = new File(layoutPath);
        String jsonStr = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        return JsonUtils.parser.fromJson(jsonStr, Layout.class);
    }

    @Override
    public void run() throws Exception {
        CorfuRuntime.CorfuRuntimeParameters runtimeParameters = getRuntimeParameters();
        Layout layout = parseLayout(layoutFile);
        BootstrapUtil.bootstrap(layout,
                runtimeParameters,
                numRetry,
                Duration.ofSeconds(retryDuration));
    }

}
