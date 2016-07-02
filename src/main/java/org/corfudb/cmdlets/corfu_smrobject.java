package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
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
    public void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Get a org.corfudb.runtime instance from the options.
        CorfuRuntime rt = configureRuntime(opts);

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
                    .filter(x -> x.getParameterCount() == (opts.get("<args>") == null ?
                            0 : ((String) opts.get("<args>")).split(",").length))
                    .findFirst().get();
        } catch (NoSuchElementException nsee) {
            throw new RuntimeException("Method " + opts.get("<method>") + " with " +
                    (opts.get("<args>") == null ?
                            0 : ((String) opts.get("<args>")).split(",").length)
                    + " arguments not found!");
        }
        if (m == null) {
            throw new RuntimeException("Method " + opts.get("<method>") + " with " +
                    (opts.get("<args>") == null ?
                            0 : ((String) opts.get("<args>")).split(",").length)
                    + " arguments not found!");
        }

        Object ret;
        try {
            ret = m.invoke(o, (opts.get("<args>") == null ?
                    null : ((String) opts.get("<args>")).split(",")));
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Couldn't invoke method on object", e);
        }

        if (ret != null) {
            System.out.println(ansi().fg(WHITE).a("Output:").reset());
            System.out.println(ret.toString());
            System.out.println(ansi().fg(GREEN).a("SUCCESS").reset());
        } else {
            System.out.println(ansi().fg(GREEN).a("SUCCESS").reset());
        }
    }
}
