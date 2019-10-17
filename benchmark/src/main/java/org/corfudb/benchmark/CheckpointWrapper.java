package org.corfudb.benchmark;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.ICorfuMap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.scenario.CorfuTableBenchmark;

@Slf4j
public class CheckpointWrapper {

    private final static int CHECKPOINT_BATCH_SIZE = 20;

    CorfuRuntime runtime;
    Token trimMark;
    Map<UUID, String> allMapNames;
    Map<UUID, CorfuTable> allMapTable;

    //Token prefixTrimAddress;

    CheckpointWrapper(CorfuRuntime runtime, Map<UUID, String> allMapNames, Map<UUID, CorfuTable> allMapTable) {
        this.runtime = runtime;
        this.trimMark = Token.UNINITIALIZED;
        this.allMapNames = allMapNames;
        this.allMapTable = allMapTable;
      }

    public void trimAndCheckpoint() {
        trim();
        trimMark = batchCheckpoint();
        log.info ("the next trimMark " + trimMark);
    }

    public void trim() {
        log.info("trimmed till " + trimMark);
        runtime.getAddressSpaceView().prefixTrim(trimMark);
        runtime.getAddressSpaceView().invalidateClientCache();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().gc();
    }

    @SuppressWarnings("unchecked")
    private Token batchCheckpoint() {
        log.info("Batch checkpoint started.");

        MultiCheckpointWriter<ICorfuMap> mcWriter = new MultiCheckpointWriter<>();

        Iterator mapNamesIt = allMapTable.entrySet().iterator();
        //List<String> mapsToLoad = new ArrayList<>();
        int count = 0;
        Token firstToken = Token.UNINITIALIZED;

        for (CorfuTable tableName : allMapTable.values ()) {
            mcWriter.addMap(tableName);
        }

        Token tmpToken = mcWriter.appendCheckpoints(runtime, getCurrentLocalDateTimeStamp());
        log.info("Batch checkpoint tmpToken " + tmpToken);
        if (firstToken == Token.UNINITIALIZED) {
            firstToken = tmpToken;
        }

        log.info("Batch checkpoint finished with count {}", count);
        clearCaches();
        System.gc();

        log.info("Batch checkpoint finished " + firstToken);
        return firstToken;
    }

    private static String getCurrentLocalDateTimeStamp() {
        return LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    private void clearCaches() {
        log.info("Clearing object view cache with size {}, stream view cache with size {}.",
                runtime.getObjectsView().getObjectCache().size(),
                runtime.getStreamsView().getStreamCache().size());

        runtime.getObjectsView().getObjectCache().clear();
        runtime.getStreamsView().getStreamCache().clear();
    }
}
