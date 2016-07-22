package org.corfudb.cmdlets;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 8/1/16.
 */
@Slf4j
public class corfu_query implements ICmdlet {

    private static final String USAGE =
            "corfu_query, query a Corfu server to get runtime information.\n"
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
        try {
            VersionInfo vi = router.getClient(BaseClient.class).getVersionInfo().get();
            Gson gs = new GsonBuilder().setPrettyPrinting().create();
            return cmdlet.ok(gs.toJson(vi));
        } catch (Exception ex) {
            return cmdlet.err(ex.toString());
        }
    }
}
