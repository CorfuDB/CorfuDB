package org.corfudb.perf;

import static java.time.temporal.ChronoUnit.SECONDS;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;

@Slf4j
public class FormCluster {

    static class Arguments {
        @Parameter(names = { "-h", "--help" }, description = "help message", help = true)
        boolean help;

        @Parameter(names = "-nodes", required = true, description = "A comma-seperated " +
                "list of nodes that are not bootstrapped (i.e. ip:port)")
        private List<String> nodes = new ArrayList<>();
    }

    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.parse(args);

        if (arguments.help) {
            jc.usage();
            System.exit(0);
        }

        Layout layout = new Layout(
                arguments.nodes,
                arguments.nodes,
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(new Layout.LayoutStripe(arguments.nodes)
                        )
                )),
                0L,
                UUID.randomUUID()
        );

        log.info("Bootstrapping {} with {}", arguments.nodes, layout);
        final int retry = 3;
        BootstrapUtil.bootstrap(layout, retry, Duration.of(10, SECONDS));
        log.info("Done!");
    }
}
