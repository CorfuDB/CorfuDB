package org.corfudb.cmdlets;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_layout implements ICmdlet {

    private static final String USAGE =
            "corfu_layout, directly interact with a layout server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_layout query <address>:<port> [-d <level>]\n"
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

        // Create a client router and get layout.
        log.trace("Creating router for {}:{}", host,port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.addClient(new LayoutClient())
                .start();
        System.out.println(ansi().a("QUERY ").fg(WHITE).a(host + ":" + port).reset().a(":"));

        try {
            Layout l = router.getClient(LayoutClient.class).getLayout().get();
            Gson gs = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(ansi().a("RESPONSE from ").fg(WHITE).a(host + ":" + port).reset().a(":"));
            System.out.println(gs.toJson(l));
        } catch (ExecutionException ex)
        {
            log.error("Exception getting layout", ex.getCause());
            throw new RuntimeException(ex.getCause());
        }
        catch (Exception e)
        {
            log.error("Exception getting layout", e);
            throw new RuntimeException(e);
        }
    }
}
