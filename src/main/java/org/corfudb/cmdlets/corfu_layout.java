package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_layout implements ICmdlet {

    private static Map<String, NettyClientRouter> routers = new ConcurrentHashMap<>();

    private static final String USAGE =
            "corfu_layout, directly interact with a layout server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_layout query <address>:<port> [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout bootstrap <address>:<port> [-l <layout>|-s] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout prepare <address>:<port> -r <rank> [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout propose <address>:<port> -r <rank> [-l <layout>|-s] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout committed <address>:<port> -r <rank> [-l <layout>] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <layout>, --layout-file=<layout>  Path to a JSON file describing the \n"
                    + "                                      desired layout. If not specified and\n"
                    + "                                      --single not specified, takes input from stdin.\n"
                    + " -s, --single                         Generate a single node layout, with the node \n"
                    + "                                      itself serving all roles.                  \n"
                    + " -r <rank>, --rank=<rank>             The rank to use for a Paxos operation. \n"
                    + " -d <level>, --log-level=<level>      Set the logging level, valid levels are: \n"
                    + "                                      ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -e <epoch>, --epoch=<epoch>          Set the epoch for the client request PDU."
                    + " -p <qapp>, --quickcheck-ap-prefix=<qapp> Set QuickCheck addressportPrefix."
                    + " -h, --help  Show this screen\n"
                    + " --version  Show version\n";

    @Override
    public String[] main(String[] args) {
        if (args != null && args.length > 0 && args[0].contentEquals("reset")) {
            LayoutServer ls = CorfuServer.getLayoutServer();
            if (ls != null) {
                ls.reset();
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active layout server");
            }
        }
        if (args != null && args.length > 0 && args[0].contentEquals("reboot")) {
            LayoutServer ls = CorfuServer.getLayoutServer();
            if (ls != null) {
                ls.reboot();
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active layout server");
            }
        }

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Parse host address and port
        String addressport = (String) opts.get("<address>:<port>");
        String host = addressport.split(":")[0];
        Integer port = Integer.parseInt(addressport.split(":")[1]);
        String qapp = (String) opts.get("<qapp>");
        String addressportPrefix = "";
        if (qapp != null) {
            addressportPrefix = qapp;
        }

        NettyClientRouter router;
        if ((router = routers.get(addressportPrefix + addressport)) == null) {
            // Create a client router and get layout.
            log.trace("Creating router for {} ++ {}:{}", addressportPrefix, port);
            router = new NettyClientRouter(host, port);
            router.addClient(new BaseClient())
                .addClient(new LayoutClient())
                .start();
            routers.putIfAbsent(addressportPrefix + addressport, router);
        }
        router = routers.get(addressportPrefix + addressport);

        if (opts.get("--epoch") != null) {
            Long epoch = Long.parseLong((String) opts.get("--epoch"));
            log.trace("Specify router's epoch as " + epoch);
            router.setEpoch(epoch);
        } else {
            try {
                Layout l = router.getClient(LayoutClient.class).getLayout().get();
                if (l != null) {
                    log.trace("Set router's epoch to " + l.getEpoch());
                    router.setEpoch(l.getEpoch());
                } else {
                    log.trace("Cannot set router's epoch");
                }
            } catch (Exception e) {
                return cmdlet.err("ERROR Exception getting initial epoch " + e.getCause());
            }
        }

        if ((Boolean) opts.get("query")) {
            try {
                Layout l = router.getClient(LayoutClient.class).getLayout().get();
                Gson gs = new GsonBuilder().setPrettyPrinting().create();
                return cmdlet.ok(gs.toJson(l));
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during query",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during query",
                            ex.getCause().toString(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("ERROR Exception getting layout" + e);
            }
        } else if ((Boolean) opts.get("bootstrap")) {
            Layout l = getLayout(opts);
            log.debug("Bootstrapping with layout {}", l);
            try {
                if (router.getClient(LayoutClient.class).bootstrapLayout(l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                return cmdlet.err("Exception bootstrapping layout", ex.getCause().toString());
            } catch (Exception e) {
                return cmdlet.err("Exception bootstrapping layout", e.toString());
            }
        } else if ((Boolean) opts.get("prepare")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            log.debug("Prepare with new rank={}", rank);
            try {
                if (router.getClient(LayoutClient.class).prepare(rank).get().isAccepted()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("ACK");
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == OutrankedException.class) {
                    OutrankedException oe = (OutrankedException) ex.getCause();
                    return cmdlet.err("Exception during prepare",
                                ex.getCause().toString(),
                                "newRank: " + Long.toString(oe.getNewRank()),
                                "layout: " + (oe.getLayout() == null ? "" : oe.getLayout().asJSONString()));
                } else if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during prepare",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during prepare",
                                ex.getCause().toString(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during prepare", e.toString(), ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("propose")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            Layout l = getLayout(opts);
            log.debug("Propose with new rank={}, layout={}", rank, l);
            try {
                if (router.getClient(LayoutClient.class).propose(rank, l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == OutrankedException.class) {
                    OutrankedException oe = (OutrankedException) ex.getCause();
                    return cmdlet.err("Exception during propose",
                            ex.getCause().toString(),
                            "newRank: " + Long.toString(oe.getNewRank()),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else if (ex.getCause().getClass() == WrongEpochException.class) {
                        WrongEpochException we = (WrongEpochException) ex.getCause();
                        return cmdlet.err("Exception during propose",
                                ex.getCause().toString(),
                                "correctEpoch: " + we.getCorrectEpoch(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during propose",
                                ex.getCause().toString(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during propose",
                            e.toString(),
                            "stack: " + ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("committed")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            Layout l = getLayout(opts);
            log.debug("Propose with new rank={}", rank);
            try {
                if (router.getClient(LayoutClient.class).committed(rank, l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during commit",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during commit",
                            ex.getCause().toString(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during commit",
                        e.toString(),
                        "stack: " + ExceptionUtils.getStackTrace(e));
            }
        }
        return cmdlet.err("Hush, compiler.");
    }

    Layout getLayout(Map<String, Object> opts) {
        Layout oLayout = null;

        if ((Boolean) opts.getOrDefault("--single", false)) {
            String localAddress = (String) opts.get("<address>:<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
            oLayout = new Layout(
                    Collections.singletonList(localAddress),
                    Collections.singletonList(localAddress),
                    Collections.singletonList(new Layout.LayoutSegment(
                            Layout.ReplicationMode.CHAIN_REPLICATION,
                            0L,
                            -1L,
                            Collections.singletonList(
                                    new Layout.LayoutStripe(
                                            Collections.singletonList(localAddress)
                                    )
                            )
                    )),
                    0L
            );
        } else if (opts.get("--layout-file") != null) {
            try {
                String f = (String) opts.get("--layout-file");
                String layoutJson = new String(Files.readAllBytes(Paths.get
                        ((String) opts.get("--layout-file"))));
                Gson g = new GsonBuilder().create();
                oLayout = g.fromJson(layoutJson, Layout.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read from file (not a JSON file?)", e);
            }
        }

        if (oLayout == null) {
            log.trace("Reading layout from stdin.");
            try {
                String s = new String(ByteStreams.toByteArray(System.in));
                Gson g = new GsonBuilder().create();
                oLayout = g.fromJson(s, Layout.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read from stdin (not valid JSON?)", e);
            }
        }

        log.debug("Parsed layout to {}", oLayout);
        return oLayout;
    }
}
