package org.corfudb.migration;

import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Layout Reset.
 * Resets the layout and epochs. This allows the node to be bootstrapped with a fresh layout.
 * This is valid for version 0.3.0.
 * This tool deletes files related to:
 * - Layout Server - Deletes the layout files, paxos phase 1 and phase 2 metadata.
 * - Management Server - Recovery layout.
 * - Sequencer Server - Epoch at which it was last bootstrapped.
 * - LogUnit Server - Epoch at which it was last reset.
 * - Netty Server Router - Server epoch to validate all message.
 * On deletion of these files, the node loses the persisted state of its IP and last seen epoch,
 * allowing a fresh startup of a cluster in cases, where log data deletion is not an option.
 * Usage:
 * java -cp migration-*-SNAPSHOT-shaded.jar org.corfudb.migration.LayoutReset corfu-dir
 */
public class LayoutReset {

    /**
     * Resets all layouts and epoch related metadata.
     *
     * @param args Takes the CorfuDB directory.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Expected parameters: CorfuDB data directory");
        }
        resetLayoutDatastore(args[0]);
    }

    /**
     * Resets all layout and epoch related metadata files.
     *
     * @param dirStr Directory path for CorfuDB.
     */
    public static void resetLayoutDatastore(String dirStr) {

        Set<String> dsKeyPrefixes = newHashSet("SERVER_EPOCH",
                "LAYOUT",
                "PHASE_1",
                "PHASE_2",
                "LAYOUTS",
                "MANAGEMENT",
                "SEQUENCER_EPOCH",
                "LOGUNIT_EPOCH_WATER_MARK");

        Arrays.stream(Objects.requireNonNull(
                new File(dirStr).listFiles(
                        (dir1, name) -> dsKeyPrefixes.stream().anyMatch(name::startsWith))))
                .forEach(File::delete);
    }
}
