package org.corfudb.perf;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final public class Utils {

    /**
     * Parse an array of string arguments into specific SimulatorArgument types
     * @param <T> an argument object to parse the string arguments into
     * @param stringArgs an array of string arguments
     */
    public static <T extends SimulatorArguments> void parse(final T arguments, final String[] stringArgs) {
        final JCommander jc = new JCommander(arguments);
        jc.parse(stringArgs);
        if (arguments.help) {
            jc.usage();
            System.exit(0);
        }
    }

    /**
     * Wrap runnable with a try/catch to log exceptions when executed in an
     * Executor.
     *
     * @param runnable runnable to execute
     * @return a wrapped runnable
     */
    public static Runnable wrapRunnable(Runnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                log.error("Runnable failed!", t);
            }
        };
    }
}
