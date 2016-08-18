package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.nio.ByteBuffer;
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
    public String[] main2(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Get a org.corfudb.runtime instance from the options.
        CorfuRuntime rt = configureRuntime(opts);

        try {
            if ((Boolean) opts.get("write")) {
                return write(rt, opts);
            } else if ((Boolean) opts.get("read")) {
                return read(rt, opts);
            }
        } catch (ExecutionException ex) {
            return cmdlet.err("Exception", ex.toString(), ex.getCause().toString());
        } catch (Exception e) {
            return cmdlet.err("Exception", e.toString(), ExceptionUtils.getStackTrace(e));
        }
        return cmdlet.err("Hush, compiler.");
    }

    String[] write(CorfuRuntime runtime, Map<String, Object> opts)
            throws Exception {
        try {
            runtime.getAddressSpaceView().write(Long.parseLong((String) opts.get("--log-address")),
                    streamsFromString((String) opts.get("--stream-ids")), ByteStreams.toByteArray(System.in),
                    Collections.emptyMap());
            return cmdlet.ok();
        } catch (OverwriteException e) {
            return cmdlet.err("OVERWRITE");
        }
    }

    String[] read(CorfuRuntime runtime, Map<String, Object> opts)
            throws Exception {
        ILogUnitEntry r = runtime.getAddressSpaceView()
                .read(Long.parseLong((String) opts.get("--log-address")));
        switch (r.getResultType()) {
            case EMPTY:
                return cmdlet.err("EMPTY");
            case FILLED_HOLE:
                return cmdlet.err("HOLE");
            case TRIMMED:
                return cmdlet.err("TRIMMED");
            case DATA:
                byte[] ba = new byte[r.getBuffer().readableBytes()];
                r.getBuffer().getBytes(0, ba);
                return cmdlet.ok(new String(ba, "UTF8"));
        }
        return cmdlet.err("Hush, compiler.");
    }
}
