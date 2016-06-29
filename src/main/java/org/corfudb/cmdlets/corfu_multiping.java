package org.corfudb.cmdlets;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.taskdefs.Sleep;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.Ansi.Color.*;
/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_multiping implements ICmdlet {

    Boolean up[] = new Boolean[99];

    private static final String USAGE =
            "corfu_multiping, pings a Corfu Server to check for connectivity to multiple servers.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_ping [-d <level>] <address>:<port> [<address>:<port> ...]\n"
                    + "\n"
                    + "Options:\n"
                    + " -d <level>, --log-level=<level>      Set the logging level, valid levels are: \n"
                    + "                                      ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -h, --help  Show this screen\n"
                    + " --version  Show version\n";


    @Override
    public void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Parse host address and port
        ArrayList<String> aps = (ArrayList<String>) opts.get("<address>:<port>");
        String hosts[] = new String[99];
        Integer ports[] = new Integer[99];
        NettyClientRouter routers[] = new NettyClientRouter[99];

        for (int i = 0; i < aps.size(); i++) {
            hosts[i] = aps.get(i).split(":")[0];
            ports[i] = Integer.parseInt(aps.get(i).split(":")[1]);
            System.out.println(ansi().a("PING ").fg(WHITE).a(hosts[i] + ":" + ports[i]).reset().a("\n"));
            routers[i] = new NettyClientRouter(hosts[i], ports[i]);
            System.out.println("Yo, eh?");
            routers[i].start();
            System.out.println("Yo, started.");
            up[i] = false;
        }

        for (int i = 0; i < 5; i++) {
            ping_one_round(hosts[0], ports[0], routers[0]);
            just_sleep_dammit(1000);
        }
        java.lang.System.exit(8);

    }

    private void just_sleep_dammit(long i) {
        try {
            java.lang.Thread.sleep(i);
        } catch (InterruptedException ie) {}
    }

    private void ping_one_round(String host, Integer port, NettyClientRouter router) {
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> cf = router.getClient(BaseClient.class).ping();
            // if an exception occurs, print it out.
            cf.exceptionally(e -> {
                if (e instanceof CompletionException) {
                    System.out.println(ansi().fg(RED).a("ERROR ").reset().a("pinging ")
                            .fg(WHITE).a(host + ":" + port).reset()
                            .a(": seq=" + -1 + " error=" +
                                    ((CompletionException) e).getCause().getClass().getSimpleName()));
                } else {
                    System.out.println(ansi().fg(RED).a("ERROR ").reset().a("pinging ")
                            .fg(WHITE).a(host + ":" + port).reset()
                            .a(": seq=" + -1 + " error=" +
                                    (e).getClass().getSimpleName()));
                }
                return null;
            });
            // when the ping completes, print the time to screen.
            cf.thenAccept((x) -> {
                long end = System.nanoTime();
                log.trace("Ping[{}] ended at {}", -1, end);
                long duration = -2;
                String ms = String.format("%.3f", (float) duration / 1000000L);
                System.out.println(ansi().a("PONG from ").fg(WHITE).a(host + ":" + port).reset()
                        .a(": seq=" + -1 + " time=" + ms + "ms"));
            });
        });

    }
}