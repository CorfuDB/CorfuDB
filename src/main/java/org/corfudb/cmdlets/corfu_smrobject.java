package org.corfudb.cmdlets;

import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;


/**
 * Created by mwei on 1/21/16.
 */
@Slf4j
public class corfu_smrobject implements ICmdlet {

    static private ConcurrentHashMap rtMap = new ConcurrentHashMap<String,CorfuRuntime>();

    private static final String USAGE =
            "corfu_smrobject, interact with SMR objects in Corfu.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_smrobject  -c <config> -s <stream-id> <class> <method> [<args>] [-d <level>] [-p <qapp>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                Usually a comma-delimited list of layout servers.\n"
                    + " -s <stream-id>, --stream-id=<stream-id>        The stream id to use. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -p <qapp>, --quickcheck-ap-prefix=<qapp>       Set QuickCheck addressportPrefix.\n"
                    + " -h, --help                                     Show this screen\n"
                    + " --version                                      Show version\n";

    @Override
    public String[] main2(String[] args) {
        if (args != null && args.length > 0 && args[0].contentEquals("reset")) {
            log.trace("corfu_smrobject top: reset");
            LogUnitServer ls = CorfuServer.getLogUnitServer();
            SequencerServer ss = CorfuServer.getSequencerServer();
            if (ls != null && ss != null) {
                log.trace("corfu_smrobject top: reset now");

                // Reset the local log server.
                ls.reset();

                // Reset the local sequencer server
                ss.reset();

                // Reset all local CorfuRuntime instances
                Iterator<String> it = rtMap.keySet().iterator();
                while (it.hasNext()) {
                    String key = it.next();
                    CorfuRuntime rt = (CorfuRuntime) rtMap.get(key);
                    // Brrrrr, state needs resetting in rt's ObjectsView
                    rt.getObjectsView().getObjectCache().clear();
                    // Brrrrr, state needs resetting in rt's AddressSpaceView
                    rt.getAddressSpaceView().resetCaches();
                    // Stop the router, sortof.  false means don't really shutdown,
                    // but disconnect any existing connection.
                    rt.stop(false);
                }
                // Avoid leak of Netty Pthread worker pool by reusing routers.
                // rtMap = new ConcurrentHashMap<String,CorfuRuntime>();

                // Reset all local CorfuRuntime StreamView
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active log server or sequencer server");
            }
        }
        if (args != null && args.length > 0 && args[0].contentEquals("reboot")) {
            log.trace("corfu_smrobject top: reboot");
            LogUnitServer ls = CorfuServer.getLogUnitServer();
            if (ls != null) {
                log.trace("corfu_smrobject top: reboot now");
                ls.reboot();
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active log server");
            }
        }

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Get a org.corfudb.runtime instance from the options.
        String config = (String) opts.get("--config");
        String qapp = (String) opts.get("--quickcheck-ap-prefix");
        String addressportPrefix = "";
        if (qapp != null) {
            addressportPrefix = qapp;
        }
        CorfuRuntime rt;
        if (rtMap.get(addressportPrefix + config) == null) {
            rt = configureRuntime(opts);
            rtMap.putIfAbsent(addressportPrefix + config, rt);
        }
        rt = (CorfuRuntime) rtMap.get(addressportPrefix + config);

        String argz = ((String) opts.get("<args>"));
        int arity;
        String[] splitz;

        if (argz == null) {
             splitz = new String[0];
            arity = 0;
        } else {
            splitz = argz.split(",");
            if (argz.charAt(argz.length() - 1) == ',') {
                arity = splitz.length + 1;
                String[] new_splitz = new String[arity];
                for (int i = 0; i < arity - 1; i++) {
                    new_splitz[i] = splitz[i];
                }
                new_splitz[arity - 1] = "";
                splitz = new_splitz;
            } else {
                arity = splitz.length;
            }
        }

        // Attempt to open the object
        Class<?> cls;
        try {
            cls = Class.forName((String) opts.get("<class>"));
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }

        Object o = rt.getObjectsView().build()
                .setStreamName((String) opts.get("--stream-id"))
                .setType(cls)
                .open();

        // Use reflection to find the method...
        Method m;
        try {
            m = Arrays.stream(cls.getDeclaredMethods())
                    .filter(x -> x.getName().equals(opts.get("<method>")))
                    .filter(x -> x.getParameterCount() == arity)
                    .findFirst().get();
        } catch (NoSuchElementException nsee) {
            return cmdlet.err("Method " + opts.get("<method>") + " with " +
                    arity
                    + " arguments not found!");
        }
        if (m == null) {
            return cmdlet.err("Method " + opts.get("<method>") + " with " +
                    arity
                    + " arguments not found!");
        }

        Object ret;
        for (int i = 0; i < 10; i++) {
            try {
                ret = m.invoke(o, splitz);
            } catch (InvocationTargetException e) {
                Throwable c = ExceptionUtils.getCause(e);
                if (c.getClass() == org.corfudb.runtime.exceptions.NetworkException.class &&
                        c.toString().matches(".*Disconnected endpoint.*")) {
                    // Very occasionally, QuickCheck tests will encounter an exception
                    // caused by a disconnection with the remote endpoint.  That kind
                    // of non-determinism is evil.  Just retry a few times via 'for' loop.
                    log.warn("WHOA, 'Disconnected endpoint', looping...\n");
                    try { Thread.sleep(50); } catch (InterruptedException ie){};
                    continue;
                } else {
                    return cmdlet.err("Couldn't invoke method on object: " + e,
                            "stack: " + ExceptionUtils.getStackTrace(e),
                            "cause: " + ExceptionUtils.getCause(e));
                }
            } catch (IllegalAccessException e) {
                return cmdlet.err("Couldn't invoke method on object: " + e,
                        "stack: " + ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                return cmdlet.err("Exception on object: " + e,
                        "stack: " + ExceptionUtils.getStackTrace(e),
                        "cause: " + ExceptionUtils.getCause(e));
            }

            if (ret != null) {
                return cmdlet.ok(ret.toString());
            } else {
                return cmdlet.ok();
            }
        }
        return cmdlet.err("Exhausted for loop retries");
    }
}
