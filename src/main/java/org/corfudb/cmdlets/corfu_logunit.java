package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
                    + "\tcorfu_logunit read <address>:<port> -a <log-address> [-d <level>]\n"
                    + "\tcorfu_logunit trim <address>:<port> -a <log-address> [-r <stream-id>] [-d <level>]\n"
                    + "\tcorfu_logunit fillHole <address>:<port> -a <log-address> [-d <level>]\n"
                    + "\tcorfu_logunit forceGC <address>:<port> [-d <level>]\n"
                    + "\tcorfu_logunit setGCInterval <address>:<port> -i <interval> [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -a <log-address>, --log-address=<log-address>  The log address to use. \n"
                    + " -i <interval>, --interval=<interval>           Interval to set garbage collection to. \n"
                    + " -s <stream-ids>, --stream-ids=<stream-ids>     The stream ids to use, comma separated. \n"
                    + " -t <stream-id>, --stream-id=<stream-id>        A stream id to use, comma separated. \n"
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
        log.trace("Creating router for {}:{}", host, port);
        NettyClientRouter router = new NettyClientRouter(host, port);
        router.addClient(new LogUnitClient())
                .start();

        try {
            if ((Boolean) opts.get("write")) {
                write(router, opts);
            } else if ((Boolean) opts.get("read")) {
                read(router, opts);
            } else if ((Boolean) opts.get("trim")) {
                trim(router, opts);
            } else if ((Boolean) opts.get("fillHole")) {
                fillHole(router, opts);
            } else if ((Boolean) opts.get("forceGC")) {
                forceGC(router, opts);
            } else if ((Boolean) opts.get("setGCInterval")) {
                setGCInterval(router, opts);
            }
        } catch (ExecutionException ex) {
            log.error("Exception", ex.getCause());
            throw new RuntimeException(ex.getCause());
        } catch (Exception e) {
            log.error("Exception", e);
            throw new RuntimeException(e);
        }
    }

    void write(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        router.getClient(LogUnitClient.class).write(Long.parseLong((String) opts.get("--log-address")),
                streamsFromString((String) opts.get("--stream-ids")), Integer.parseInt((String) opts.get("--rank")),
                ByteStreams.toByteArray(System.in), Collections.emptyMap()).get();
    }

    void trim(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        router.getClient(LogUnitClient.class).trim(getUUIDfromString((String) opts.get("--stream-id")),
                Long.parseLong((String) opts.get("--log-address")));
    }

    void fillHole(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        router.getClient(LogUnitClient.class).fillHole(
                Long.parseLong((String) opts.get("--log-address"))).get();
    }

    void forceGC(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        router.getClient(LogUnitClient.class).forceGC();
    }

    void setGCInterval(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        router.getClient(LogUnitClient.class).setGCInterval(Long.parseLong((String) opts.get("--interval")));
    }

    void read(NettyClientRouter router, Map<String, Object> opts)
            throws Exception {
        ReadResponse r = router.getClient(LogUnitClient.class).read(Long.parseLong((String) opts.get("--log-address"))).get();
        r.getReadSet().entrySet().stream().forEach(x -> {
            switch (x.getValue().getType()) {
                case EMPTY:
                    System.err.println("Error: EMPTY");
                    break;
                case HOLE:
                    System.err.println("Error: HOLE");
                    break;
                case TRIMMED:
                    System.err.println("Error: TRIMMED");
                    break;
                case DATA:
                    try {
                        x.getValue().getData().getBytes(0, System.out, x.getValue().getData().readableBytes());
                    } catch (IOException i) {
                        System.err.println("Error: IOException " + i.getMessage());
                    }
                    break;
            }
        });
    }
}
