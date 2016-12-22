package org.corfudb.util.quickcheck;

import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.util.quickcheck.QCUtil.configureRuntime;
import static org.corfudb.util.quickcheck.QCUtil.replyErr;
import static org.corfudb.util.quickcheck.QCUtil.replyOk;

public class QCSMRobject {

    static private ConcurrentHashMap rtMap = new ConcurrentHashMap<String,CorfuRuntime>();

    private static final String USAGE =
            "quickcheck interface legacy code.\n"
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

    public static String[] main(String[] args) {
        if (args != null && args.length > 0 && args[0].contentEquals("reboot")) {
            ManagementServer ms = CorfuServer.getManagementServer();
            ms.shutdown();
            CorfuServer.addManagementServer();

            LogUnitServer ls = CorfuServer.getLogUnitServer();
            if (ls != null) {
                ls.shutdown();
                CorfuServer.addLogUnit();
                return replyOk();
            } else {
                return replyErr("No active log server");
            }
        }

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

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
        String[] split;

        if (argz == null) {
             split = new String[0];
            arity = 0;
        } else {
            split = argz.split(",");
            if (argz.charAt(argz.length() - 1) == ',') {
                arity = split.length + 1;
                String[] new_split = new String[arity];
                for (int i = 0; i < arity - 1; i++) {
                    new_split[i] = split[i];
                }
                new_split[arity - 1] = "";
                split = new_split;
            } else {
                arity = split.length;
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
            return replyErr("Method " + opts.get("<method>") + " with " +
                    arity
                    + " arguments not found!");
        }
        if (m == null) {
            return replyErr("Method " + opts.get("<method>") + " with " +
                    arity
                    + " arguments not found!");
        }

        Object ret;
        final int c10 = 10, c50 = 50;
        for (int i = 0; i < c10; i++) {
            try {
                ret = m.invoke(o, split);
            } catch (InvocationTargetException e) {
                Throwable c = ExceptionUtils.getCause(e);
                if (c.getClass() == org.corfudb.runtime.exceptions.NetworkException.class &&
                        c.toString().matches(".*Disconnected endpoint.*")) {
                    // Very occasionally, QuickCheck tests will encounter an exception
                    // caused by a disconnection with the remote endpoint.
                    try { Thread.sleep(c50); } catch (InterruptedException ie){};
                    continue;
                } else {
                    return replyErr("exception", e.getClass().getSimpleName(),
                            "stack: " + ExceptionUtils.getStackTrace(e),
                            "cause: " + ExceptionUtils.getCause(e));
                }
            } catch (IllegalAccessException e) {
                return replyErr("exception", e.getClass().getSimpleName(),
                        "stack: " + ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                return replyErr("Exception on object: " + e,
                        "stack: " + ExceptionUtils.getStackTrace(e),
                        "cause: " + ExceptionUtils.getCause(e));
            }

            if (ret != null) {
                return replyOk(ret.toString());
            } else {
                return replyOk();
            }
        }
        return replyErr("Exhausted for loop retries");
    }
}
