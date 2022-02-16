package org.corfudb.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.util.JsonUtils.prettyPrint;

@Slf4j
@Parameters(commandDescription = "Get cluster layout")
public class GetLayoutCommand extends AuthenticationArgs {

    @Parameter(names = "--connect", description = "Comma separated node addresses.", required = true)
    private String connection;

    @Override
    public void run() throws Exception {
        CorfuRuntime.CorfuRuntimeParameters runtimeParameters = getRuntimeParameters();
        CorfuRuntime runtime = CorfuRuntime
                .fromParameters(runtimeParameters)
                .parseConfigurationString(connection)
                .connect();

        Layout layout = runtime.getLayoutView().getLayout();
        log.info("Layout {}", prettyPrint(layout.asJSONString()));
    }

}
