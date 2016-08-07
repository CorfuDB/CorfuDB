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

import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 8/1/16.
 */
@Slf4j
public class corfu_reset implements ICmdlet {

    private static final String USAGE =
            "corfu_reset, reset a Corfu server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_reset <address>:<port> [-d <level>]\n"
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
        String addressport = (String) opts.get("<address>:<port>");
        String host = addressport.split(":")[0];
        Integer port = Integer.parseInt(addressport.split(":")[1]);

        // Create a client router and ping.
        log.trace("Creating router for {}:{}", host, port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.start();
        System.out.println(ansi().a("RESET ").fg(WHITE).a(host + ":" + port).reset().a(":"));
        try {
            if (router.getClient(BaseClient.class).reset().get()) {
                System.out.println(ansi().fg(WHITE).a("ACK").reset());
            }
            else {
                System.out.println(ansi().fg(RED).a("NACK").reset());
            }
        } catch (Exception ex) {
            System.out.println(ansi().fg(RED).a("ERROR").reset().a(":"));
            System.out.println(ex.toString());
        }
    }
}
