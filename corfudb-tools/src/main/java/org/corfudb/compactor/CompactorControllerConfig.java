package org.corfudb.compactor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * CorfuCompactorControlsConfig class parses the compactor controls specific parameters passed if any
 */
@Getter
@Slf4j
public class CompactorControllerConfig extends CompactorBaseConfig {
    private final boolean upgradeDescriptorTable;
    private final boolean instantTriggerCompaction;
    private final boolean trim;
    private final boolean freezeCompaction;
    private final boolean unfreezeCompaction;
    private final boolean disableCompaction;
    private final boolean enableCompaction;

    public CompactorControllerConfig(String[] args) {
        super(args, CompactorControlsCmdLineHelper.USAGE_PARAMS, CompactorControlsCmdLineHelper.OPTIONS_PARAMS);

        upgradeDescriptorTable = getOpt("--upgradeDescriptorTable").isPresent();
        instantTriggerCompaction = getOpt("--instantTriggerCompaction").isPresent();
        trim = getOpt("--trim").isPresent();
        freezeCompaction = getOpt("--freezeCompaction").isPresent();
        unfreezeCompaction = getOpt("--unfreezeCompaction").isPresent();
        disableCompaction = getOpt("--disableCompaction").isPresent();
        enableCompaction = getOpt("--enableCompaction").isPresent();

        if (freezeCompaction && unfreezeCompaction) {
            log.error("Both freeze and unfreeze compaction parameters cannot be passed together");
            throw new IllegalArgumentException("Both freeze and unfreeze compaction parameters cannot be passed together");
        }
        if (disableCompaction && enableCompaction) {
            log.error("Both enable and disable compaction parameters cannot be passed together");
            throw new IllegalArgumentException("Both enable and disable compaction parameters cannot be passed together");
        }
    }

    public static class CompactorControlsCmdLineHelper {
        public static final String USAGE_PARAMS = " [--trim=<trim>] " +
                "[--upgradeDescriptorTable=<upgradeDescriptorTable>] " +
                "[--instantTriggerCompaction=<instantTriggerCompaction>] " +
                "[--freezeCompaction=<freezeCompaction>] " +
                "[--unfreezeCompaction=<unfreezeCompaction>] " +
                "[--disableCompaction=<disableCompaction>] " +
                "[--enableCompaction=<enableCompaction>]";
        public static final String OPTIONS_PARAMS = "--trim=<trim> Should trim be performed along with instantTrigger\n"
                + "--upgradeDescriptorTable=<upgradeDescriptorTable> Repopulate descriptor table?\n"
                + "--instantTriggerCompaction=<instantTriggerCompaction> If compactor cycle needs to be triggered instantly\n"
                + "--freezeCompaction=<freezeCompaction> If compaction needs to be frozen\n"
                + "--unfreezeCompaction=<unfreezeCompaction> If compaction needs to be resumed\n"
                + "--disableCompaction=<disableCompaction> If compaction needs to be disabled\n"
                + "--enableCompaction=<enableCompaction> If compaction needs to be enabled";
    }
}
