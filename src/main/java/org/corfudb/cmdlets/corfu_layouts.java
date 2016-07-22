package org.corfudb.cmdlets;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.util.ArrayList;
import java.util.Map;

import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 1/10/16.
 */
@Slf4j
public class corfu_layouts implements ICmdlet {

    private static final String USAGE =
            "corfu_layouts, work on a layout view.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_layouts query -c <config> [-d <level>]\n"
                    + "\tcorfu_layouts add_layout -c <config> -e <address> [-d <level>]\n"
                    + "\tcorfu_layouts add_sequencer -c <config> -e <address> [-d <level>]\n"
                    + "\tcorfu_layouts edit_segment <index> <stripe> -c <config> [-a -e <address> [-s <index>] | -r -s <index>] [-m <mode>] [-d <level>]\n"
                    + "\tcorfu_layouts add_stripe <index> -c <config> -e <address> [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -a, --add                               Add an endpoint to this segment.\n"
                    + " -r, --remove                            Remove an endpoint from this segment.\n"
                    + " -s <index>, --segment-index=<index>     Index of the server in the segment.\n"
                    + "                                         If not set when adding a server, the server will be added to the end.\n"
                    + " -c <config>, --config=<config>          The config string to pass to the org.corfudb.runtime. \n"
                    + "                                         Usually a comma-delimited list of layout servers.\n"
                    + " -e <address>, --endpoint=<address>      The address of the endpoint to add, in address:port form. \n"
                    + " -d <level>, --log-level=<level>         Set the logging level, valid levels are: \n"
                    + "                                         ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -m <mode>, --replication-mode=<mode>    Set the replication mode for this segment. Valid modes are: \n"
                    + "                                         CHAIN_REPLICATION, QUORUM_REPLICATION, NO_REPLICATION.\n"
                    + " -h, --help                              Show this screen\n"
                    + " --version                               Show version\n";

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
            if ((Boolean) opts.get("query")) {
                query(rt, opts);
            } else if ((Boolean) opts.get("add_layout")) {
                add_layout(rt, opts);
            } else if ((Boolean) opts.get("add_sequencer")) {
                add_sequencer(rt, opts);
            } else if ((Boolean) opts.get("edit_segment")) {
                edit_segment(rt, opts);
            } else if ((Boolean) opts.get("add_stripe")) {
                add_stripe(rt, opts);
            }
        } catch (Exception e) {
            log.error("Exception", e);
            throw new RuntimeException(e);
        }
        return cmdlet.err("FIXME 9");
    }

    public void query(CorfuRuntime runtime, Map<String, Object> options) {
        Layout l = runtime.getLayoutView().getLayout();
        Gson gs = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(ansi().a("RESPONSE").fg(WHITE).reset().a(":"));
        System.out.println(gs.toJson(l));
    }

    public void add_layout(CorfuRuntime runtime, Map<String, Object> options)
            throws NetworkException, QuorumUnreachableException, OutrankedException, AlreadyBootstrappedException {
        checkEndpoint((String) options.get("--endpoint"));
        Layout l;
        Layout lPrev = runtime.getLayoutView().getCurrentLayout();
        try {
            l = (Layout) lPrev.clone();
            l.setRuntime(runtime);
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
        // increment the epoch by 1, and try installing the new layout.
        l.setEpoch(l.getEpoch() + 1);
        log.info("Updating epoch, old={}, new={}", l.getEpoch() - 1, l.getEpoch());
        l.getLayoutServers().add((String) options.get("--endpoint"));
        // bootstrap the new layout server with this configuration.
        log.info("Bootstrap layout server at {}", options.get("--endpoint"));
        runtime.getRouter((String) options.get("--endpoint")).getClient(LayoutClient.class)
                .bootstrapLayout(l);
        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, l.getEpoch());
        log.info("Layout server at {} added to layout.", options.get("--endpoint"));
    }

    public void add_sequencer(CorfuRuntime runtime, Map<String, Object> options)
            throws NetworkException, QuorumUnreachableException, OutrankedException {
        checkEndpoint((String) options.get("--endpoint"));
        Layout l;
        Layout lPrev = runtime.getLayoutView().getCurrentLayout();
        try {
            l = (Layout) lPrev.clone();
            l.setRuntime(runtime);
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
        // increment the epoch by 1, and try installing the new layout.
        l.setEpoch(l.getEpoch() + 1);
        log.info("Updating epoch, old={}, new={}", l.getEpoch() - 1, l.getEpoch());
        l.getSequencers().add((String) options.get("--endpoint"));

        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, l.getEpoch());
        log.info("Sequencer server at {} added to layout.", options.get("--endpoint"));
    }

    public void add_stripe(CorfuRuntime runtime, Map<String, Object> options)
            throws NetworkException, QuorumUnreachableException, OutrankedException {
        checkEndpoint((String) options.get("--endpoint"));
        Layout l;
        Layout lPrev = runtime.getLayoutView().getCurrentLayout();
        try {
            l = (Layout) lPrev.clone();
            l.setRuntime(runtime);
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
        // increment the epoch by 1, and try installing the new layout.
        l.setEpoch(l.getEpoch() + 1);
        log.info("Updating epoch, old={}, new={}", l.getEpoch() - 1, l.getEpoch());
        ArrayList<String> lus = new ArrayList<>();
        lus.add(Utils.getOption(options, "--endpoint", String.class));
        Layout.LayoutStripe ls = new Layout.LayoutStripe(lus);
        l.getSegments().get(Utils.getOption(options, "<index>", Integer.class)).getStripes()
                .add(ls);

        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, l.getEpoch());
        log.info("Layout server at {} added to new stripe.", options.get("--endpoint"));
    }

    public void edit_segment(CorfuRuntime runtime, Map<String, Object> options)
            throws NetworkException, QuorumUnreachableException, OutrankedException {
        Layout l;
        Layout lPrev = runtime.getLayoutView().getCurrentLayout();
        try {
            l = (Layout) lPrev.clone();
            l.setRuntime(runtime);
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }

        // increment the epoch by 1, and try installing the new layout.
        l.setEpoch(l.getEpoch() + 1);
        log.info("Updating epoch, old={}, new={}", l.getEpoch() - 1, l.getEpoch());

        int segmentIndex = Integer.parseInt((String) options.get("<index>"));

        // Set the new replication mode.
        if (options.get("--replication-mode") != null) {
            l.getSegments().get(segmentIndex).setReplicationMode(
                    Layout.ReplicationMode.valueOf((String) options.get("--replication-mode")));
        }

        // Remove a server from the segment.
        if ((Boolean) options.get("--remove")) {
            int serverIndex = Integer.parseInt((String) options.get("--segment-index"));
            log.info("Removing server {} from segment {}", l.getSegments().get(segmentIndex).getStripes().get(0).getLogServers()
                    .get(serverIndex), segmentIndex);
            l.getSegments().get(segmentIndex).getStripes().get(0).getLogServers()
                    .remove(serverIndex);
            log.info("New server set is {}", l.getSegments().get(segmentIndex).getStripes().get(0).getLogServers());
        }

        // Add a server to the segment.
        if ((Boolean) options.get("--add")) {
            checkEndpoint((String) options.get("--endpoint"));
            Integer serverIndex = options.get("--segment-index") == null ?
                    null : Integer.parseInt((String) options.get("--segment-index"));
            if (serverIndex == null) {
                log.info("Adding server {} to end of segment {}", options.get("--endpoint"), segmentIndex);
                l.getSegments().get(segmentIndex).getStripes().get(Utils.getOption(options, "<stripe>", Integer.class)).getLogServers().add((String) options.get("--endpoint"));
            } else {
                log.info("Adding server {} to segment {} at index {}", options.get("--endpoint"), segmentIndex, serverIndex);
                l.getSegments().get(segmentIndex).getStripes().get(Utils.getOption(options, "<stripe>", Integer.class)).getLogServers().add(serverIndex, (String) options.get("--endpoint"));
            }
            log.info("New server set is {}", l.getSegments().get(segmentIndex).getStripes().get(0).getLogServers());
        }

        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, l.getEpoch());
        log.info("Segment {} edited in layout.", segmentIndex);
    }
}
