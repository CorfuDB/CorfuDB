package org.corfudb.cmdlets;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class corfu_sequencer implements ICmdlet {
    private static final String USAGE =
            "corfu_sequencer, directly interact with a sequencer server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_sequencer token <address>:<port> [-s <stream-ids>] [-n <num-tokens>] [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -n <num-tokens>, --num-tokens=<num-tokens>     Number of tokens to request, or 0 for current.\n"
                    + "                                                [default: 0].                                 \n"
                    + " -s <stream-ids>, --stream-ids=<stream-ids>     The stream ids to use, comma separated. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
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

        // Create a client router and get layout.
        log.trace("Creating router for {}:{}", host, port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.addClient(new SequencerClient())
                .start();
        System.out.println(ansi().a("TOKEN_REQUEST ").fg(WHITE).a(host + ":" + port).reset().a(":"));

        try {
            long token = router.getClient(SequencerClient.class).nextToken(
                    streamsFromString((String) opts.get("--stream-ids")),
                    Integer.parseInt((String) opts.get("--num-tokens"))).get().getToken();
            System.out.println(ansi().a("RESPONSE from ").fg(WHITE).a(host + ":" + port).reset().a(":"));
            System.out.println(token);
        } catch (ExecutionException ex) {
            log.error("Exception getting sequence", ex.getCause());
            throw new RuntimeException(ex.getCause());
        } catch (Exception e) {
            log.error("Exception getting layout", e);
            throw new RuntimeException(e);
        }
        return cmdlet.err("FIXME 2");
    }
}
