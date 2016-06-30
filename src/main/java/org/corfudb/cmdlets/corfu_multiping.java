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

    int num;
    String hosts[] = new String[99];
    Integer ports[] = new Integer[99];
    NettyClientRouter routers[] = new NettyClientRouter[99];
    Boolean up[]      = new Boolean[99];
    Boolean last_up[] = new Boolean[99];

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
        num = aps.size();
        for (int i = 0; i < aps.size(); i++) {
            hosts[i] = aps.get(i).split(":")[0];
            ports[i] = Integer.parseInt(aps.get(i).split(":")[1]);
            // Don't call $router.start() now because we'll get an exception
            // if that server isn't running & responding to pings right now.
            routers[i] = null;
            up[i] = last_up[i] = false;
        }

        int c = 0;
        while (true) {
            log.info("Main top, c = " + c);
            ping_one_round(c);
            just_sleep_dammit(1000);
            // just_sleep_dammit(2);
            // This kind of check may not see flapping that happens during
            // intervals of less than this loop's polling interval.
            for (int j = 0; j < num; j++) {
                if (last_up[j] != up[j]) {
                    // System.out.println("Host " + hosts[j] + " port " + ports[j] + ": " +
                    //                    last_up[j] + " -> " + up[j]);
                    log.info("Host " + hosts[j] + " port " + ports[j] + ": " +
                            last_up[j] + " -> " + up[j]);

                    last_up[j] = up[j];
                }
            }
            c++;
        }
        // notreached
    }

    private void just_sleep_dammit(long i) {
        try {
            java.lang.Thread.sleep(i);
        } catch (InterruptedException ie) {}
    }

    private void ping_one_round(long c) {
        for (int i = 0; i < num; i++) {
            ping_host_once(c, i);
        }
    }

    private void ping_host_once(long c, int nth) {
        // This mutable data stuff gives me the heebie-jeebies.....
        //
        // It would be very nice to avoid recreating a new router & new connection
        // each time we want to send a single PING msg.  But these CompletableFuture
        // things may (may not?) run in different threads, and being racy
        // How the hell do I know that this is safe?  I don't know, period.
        // So I'll close my eyes and assume that anything bad that might happen
        // is happening in a try/catch thingie and nothing explosive leaks out.

        CompletableFuture.runAsync(() -> {
            if (routers[nth] == null) {
                // System.out.println(hosts[nth] + " port " + ports[nth] + " new router.");
                NettyClientRouter r = new NettyClientRouter(hosts[nth], ports[nth]);
                r.start();
                routers[nth] = r;
                routers[nth].setTimeoutConnect(50);
                routers[nth].setTimeoutRetry(700);
                routers[nth].setTimeoutResponse(4500);
            }
            CompletableFuture<Boolean> cf = routers[nth].getClient(BaseClient.class).ping();
            cf.exceptionally(e -> {
                log.info(hosts[nth] + " port " + ports[nth] + " c " + c + " exception " + e);
                up[nth] = false;
                // routers[nth] = null;
                return false;
            });
            cf.thenAccept((x) -> {
                log.info(hosts[nth] + " port " + ports[nth] + " c " + c + " " + x);
                if (x == true) {
                    up[nth] = true;
                } else {
                    up[nth] = false;
                    // routers[nth] = null;
                }
                return;
            });
        });

    }
}