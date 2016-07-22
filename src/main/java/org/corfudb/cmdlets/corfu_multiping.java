package org.corfudb.cmdlets;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.taskdefs.Sleep;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.Utils;
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
    String hosts[];
    Integer ports[];
    NettyClientRouter routers[];
    Boolean up[];
    Boolean last_up[];

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
    public String[] main2(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Parse host address and port
        ArrayList<String> aps = (ArrayList<String>) opts.get("<address>:<port>");
        num = aps.size();
        hosts = new String[num];
        ports = new Integer[num];
        routers = new NettyClientRouter[num];
        up = new Boolean[num];
        last_up = new Boolean[num];

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
            ping_one_round(c);
            sleep_wrapper(1000);
            // We may not see flapping that happens during
            // intervals of less than this loop's polling interval.
            for (int j = 0; j < num; j++) {
                if (last_up[j] != up[j]) {
                    log.info(hosts[j] + ":" + ports[j] + " " + last_up[j] + " -> " + up[j]);
                    last_up[j] = up[j];
                }
            }
            c++;
        }
        // notreached
        // return cmdlet.err("FIXME 6");
    }

    private void ping_one_round(long c) {
        for (int i = 0; i < num; i++) {
            ping_host_once(c, i);
        }
    }

    private void ping_host_once(long c, int nth) {
        // This mutable data stuff gives me the heebie-jeebies.....
        CompletableFuture.runAsync(() -> {
            if (routers[nth] == null) {
                NettyClientRouter r = new NettyClientRouter(hosts[nth], ports[nth]);
                r.start(c);
                routers[nth] = r;
                routers[nth].setTimeoutConnect(50);
                routers[nth].setTimeoutRetry(200);
                routers[nth].setTimeoutResponse(1000);
            }
            try {
               CompletableFuture<Boolean> cf = routers[nth].getClient(BaseClient.class).ping();
                cf.exceptionally(e -> {
                    log.trace(hosts[nth] + " port " + ports[nth] + " c " + c + " exception " + e);
                    up[nth] = false;
                    return false;
                });
                cf.thenAccept((x) -> {
                    log.trace(hosts[nth] + " port " + ports[nth] + " c " + c + " " + x);
                    if (x == true) {
                        up[nth] = true;
                    } else {
                        up[nth] = false;
                    }
                    return;
                });

            } catch (Exception e) {
                log.trace("Ping failed due to exception", e);
                up[nth] = false;
                return;
            }
            return;
        });
    }

    private void sleep_wrapper(long i) {
        try {
            java.lang.Thread.sleep(i);
        } catch (InterruptedException ie) {}
    }
}
