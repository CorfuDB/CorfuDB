package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 1/21/16.
 */
public class corfu_smrobject implements ICmdlet {

    static private CorfuRuntime rt = null;

    private static final String USAGE =
            "corfu_smrobject, interact with SMR objects in Corfu.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_smrobject  -c <config> -s <stream-id> <class> <method> [<args>] [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                Usually a comma-delimited list of layout servers.\n"
                    + " -s <stream-id>, --stream-id=<stream-id>        The stream id to use. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -h, --help                                     Show this screen\n"
                    + " --version                                      Show version\n";

    @Override
    public String[] main2(String[] args) {
        if (args != null && args.length > 0 && args[0].contentEquals("reset")) {
            if (rt != null) {
                rt.stop();
            }
            rt = null;
            return cmdlet.ok();
        }

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Get a org.corfudb.runtime instance from the options.
        if (rt == null) {
            rt = configureRuntime(opts);
        }

        String argz = ((String) opts.get("<args>"));
        int arity;
        String[] splitz = null;

        if (argz == null) {
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
        try {
            ret = m.invoke(o, splitz);
        } catch (IllegalAccessException | InvocationTargetException e) {
            return cmdlet.err("Couldn't invoke method on object" + e);
        }

        if (ret != null) {
            return cmdlet.ok(ret.toString());
        } else {
            return cmdlet.ok();
        }
    }
}
