package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class corfu_stream implements ICmdlet {
    private static final String USAGE =
            "corfu_stream, interact with streams in Corfu.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_stream write -c <config> -i <stream-ids> [-d <level>]\n"
                    + "\tcorfu_stream read -c <config> -s <stream-id> [-l] [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                Usually a comma-delimited list of layout servers.\n"
                    + " -a <log-address>, --log-address=<log-address>  The log address to use. \n"
                    + " -s <stream-id>, --stream-id=<stream-id>        The stream id to use. \n"
                    + " -l, --loop                                     Keep reading even after the stream ends. \n"
                    + " -i <stream-ids>, --stream-ids=<stream-ids>     The stream ids to use, comma separated. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -h, --help                                     Show this screen\n"
                    + " --version                                      Show version\n";

    @Override
    public void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Get a org.corfudb.runtime instance from the options.
        CorfuRuntime rt = configureRuntime(opts);

        try {
            if ((Boolean) opts.get("write")) {
                write(rt, opts);
            } else if ((Boolean) opts.get("read")) {
                read(rt, opts);
            }
        } catch (ExecutionException ex) {
            log.error("Exception", ex.getCause());
            throw new RuntimeException(ex.getCause());
        } catch (Exception e) {
            log.error("Exception", e);
            throw new RuntimeException(e);
        }
    }

    void write(CorfuRuntime runtime, Map<String, Object> opts)
            throws Exception {
        runtime.getStreamsView().write(streamsFromString((String) opts.get("--stream-ids")),
                ByteStreams.toByteArray(System.in));
    }

    void read(CorfuRuntime runtime, Map<String, Object> opts)
            throws Exception {
        StreamView s = runtime.getStreamsView().get(getUUIDfromString((String) opts.get("--stream-id")));
        while (true) {
            LogData r = s.read();
            if (r == null) {
                if (!(Boolean) opts.get("--loop")) {
                    return;
                }
                Thread.sleep(100);
            } else {
                r.getData().getBytes(0, System.out, r.getData().readableBytes());
            }
        }
    }

}
