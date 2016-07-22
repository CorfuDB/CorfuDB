package org.corfudb.cmdlets;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_ping implements ICmdlet {

    private static final String USAGE =
            "corfu_ping, pings a Corfu Server to check for connectivity.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_ping <address>:<port> [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -d <level>, --log-level=<level>      Set the logging level, valid levels are: \n"
                    + "                                      ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -h, --help  Show this screen\n"
                    + " --version  Show version\n";


    @Override
    public String[] main2(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Parse host address and port
        String addressport = (String) opts.get("<address>:<port>");
        String host = addressport.split(":")[0];
        Integer port = Integer.parseInt(addressport.split(":")[1]);

        // Create a client router and ping.
        log.trace("Creating router for {}:{}", host, port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.start();
        System.out.println(ansi().a("PING ").fg(WHITE).a(host + ":" + port).reset().a(":"));

        // Ping endlessly.
        AtomicLong sequence = new AtomicLong();
        while (true) {
            // This completable runs a unit of ping
            CompletableFuture.runAsync(() -> {
                // take the start time and a sequence number.
                long start = System.nanoTime();
                long seqNo = sequence.getAndIncrement();
                log.trace("Ping[{}] started at {}", seqNo, start);
                // ping the endpoint.
                CompletableFuture<Boolean> cf = router.getClient(BaseClient.class).ping();
                // if an exception occurs, print it out.
                cf.exceptionally(e -> {
                    if (e instanceof CompletionException) {
                        System.out.println(ansi().fg(RED).a("ERROR ").reset().a("pinging ")
                                .fg(WHITE).a(host + ":" + port).reset()
                                .a(": seq=" + seqNo + " error=" +
                                        ((CompletionException) e).getCause().getClass().getSimpleName()));
                    } else {
                        System.out.println(ansi().fg(RED).a("ERROR ").reset().a("pinging ")
                                .fg(WHITE).a(host + ":" + port).reset()
                                .a(": seq=" + seqNo + " error=" +
                                        (e).getClass().getSimpleName()));
                    }
                    return null;
                });
                // when the ping completes, print the time to screen.
                cf.thenAccept((x) -> {
                    long end = System.nanoTime();
                    log.trace("Ping[{}] ended at {}", seqNo, end);
                    long duration = end - start;
                    String ms = String.format("%.3f", (float) duration / 1000000L);
                    System.out.println(ansi().a("PONG from ").fg(WHITE).a(host + ":" + port).reset()
                            .a(": seq=" + seqNo + " time=" + ms + "ms"));
                });
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
        // return cmdlet.err("FIXME 5");
    }
}
