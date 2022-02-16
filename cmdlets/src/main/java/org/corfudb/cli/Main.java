package org.corfudb.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static java.lang.System.exit;

/**
 * Created by mwei on 11/18/16.
 */
@Slf4j
public class Main {

    @Parameter(names = {"-h", "--help"}, help = true, description = "Show help.")
    boolean help;

    @Parameter(names = "--debug", description = "Debug mode.")
    private boolean debug = false;

    public static void main(String[] args) throws Exception {
        Main main = new Main();

        JCommander jc = JCommander.newBuilder()
                .addObject(main)
                .addCommand("bootstrap", new BootstrapCommand())
                .addCommand("get-layout", new GetLayoutCommand())
                .build();

        jc.parse(args);

        String cmd = jc.getParsedCommand();
        if (Objects.isNull(cmd) || main.help) {
            jc.usage();
            exit(1);
        }

        if (main.debug) {
            Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            root.setLevel(Level.DEBUG);
        }

        BaseCommand commandObject = (BaseCommand) jc.getCommands().get(cmd).getObjects().get(0);

        commandObject.run();
    }
}
