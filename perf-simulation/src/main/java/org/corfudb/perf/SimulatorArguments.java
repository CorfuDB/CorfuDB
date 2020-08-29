package org.corfudb.perf;

import com.beust.jcommander.Parameter;

public abstract class SimulatorArguments {
    @Parameter(names = { "-h", "--help" }, description = "help message", help = true)
    protected boolean help;
}
