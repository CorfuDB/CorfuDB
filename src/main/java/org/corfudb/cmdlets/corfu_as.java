package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class corfu_as implements ICmdlet {
    private static final String USAGE =
            "corfu_as, interact with the address space view.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_as write -c <config> -a <log-address> [-s <stream-ids>] [-d <level>]\n"
                    + "\tcorfu_as read -c <config> -a <log-address> [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                Usually a comma-delimited list of layout servers.\n"
                    + " -a <log-address>, --log-address=<log-address>  The log address to use. \n"
                    + " -s <stream-ids>, --stream-ids=<stream-ids>     The stream ids to use, comma separated. \n"
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
        runtime.getAddressSpaceView().write(Long.parseLong((String) opts.get("--log-address")),
                streamsFromString((String) opts.get("--stream-ids")), ByteStreams.toByteArray(System.in),
                Collections.emptyMap());
    }

    void read(CorfuRuntime runtime, Map<String, Object> opts)
            throws Exception {
        LogData r = runtime.getAddressSpaceView()
                .read(Long.parseLong((String) opts.get("--log-address")));
        switch (r.getType()) {
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
                r.getData().getBytes(0, System.out, r.getData().readableBytes());
                break;
        }
    }
}
