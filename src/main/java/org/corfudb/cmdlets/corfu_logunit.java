package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.protocols.LayoutClient;
import org.corfudb.runtime.protocols.LogUnitClient;
import org.corfudb.runtime.protocols.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_logunit implements ICmdlet {

    private static final String USAGE =
            "corfu_logunit, directly interact with a logunit server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_logunit write <address>:<port> -a <log-address> [-s <stream-ids>] [-r <rank>] [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -a <log-address>, --log-address=<log-address>  The log address to use. \n"
                    + " -s <stream-ids>, --stream-ids=<stream-ids>     The stream ids to use, comma separated. \n"
                    + " -r <rank>, --rank=<rank>                       The rank to use [default: 0]. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
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
        log.trace("Creating router for {}:{}", host,port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.addClient(new LogUnitClient())
                .start();

        try {
            router.getClient(LogUnitClient.class).write(Long.parseLong((String) opts.get("--log-address")),
                    Collections.emptySet(), Integer.parseInt((String) opts.get("--rank")),
                    ByteStreams.toByteArray(System.in)).get();
        } catch (ExecutionException ex)
        {
            log.error("Exception writing", ex.getCause());
            throw new RuntimeException(ex.getCause());
        }
        catch (Exception e)
        {
            log.error("Exception writing", e);
            throw new RuntimeException(e);
        }
    }
}
