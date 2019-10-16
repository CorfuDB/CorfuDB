package org.corfudb.benchmark;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.ICorfuMap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckpointWrapper {

    private final static int CHECKPOINT_BATCH_SIZE = 20;

    CorfuRuntime runtime;
    Token trimMark;

    CheckpointWrapper(CorfuRuntime runtime) {
        this.runtime = runtime;
        //trimMark = -1;
    }

    public void trimAndCheckpoint() {
        //call trim();
        //batchCheckpoint();
    }

    public Token batchCheckpoint(Map<UUID, String> allMapNames) {
        log.info ("Batch checkpoint started.");
        MultiCheckpointWriter<ICorfuMap> mcWriter = new MultiCheckpointWriter<> ();

        Iterator mapNamesIt = allMapNames.entrySet ().iterator ();
        List<String> mapsToLoad = new ArrayList<> ();
        int count = 0;
        Token firstToken = Token.UNINITIALIZED;

        while (mapNamesIt.hasNext ()) {
            Map.Entry<UUID, String> entry = (Map.Entry<UUID, String>) mapNamesIt.next ();

            // Meet batch size or last, do the checkpoint.
            if (mapsToLoad.size () == CHECKPOINT_BATCH_SIZE ||
                    (!mapNamesIt.hasNext () && mapsToLoad.size () > 0)) {
                // Load to fast loader.
                // fastLoadMaps(fastLoader, mapsToLoad);

                // Batch checkpoint.
                for (String mapName : mapsToLoad) {
                    //mcWriter.addMap(dataStore.getTable(mapName));
                }

                Token tmpToken = mcWriter.appendCheckpoints (runtime, getCurrentLocalDateTimeStamp ());

                if (firstToken == Token.UNINITIALIZED) {
                    firstToken = tmpToken;
                }

                log.info ("Batch checkpoint finished with count {}", count);

                // Clear up for each batch.
                mcWriter = new MultiCheckpointWriter<> ();
                // fastLoader = dataStore.newFastObjectLoader();
                mapsToLoad.clear ();
                clearCaches();
                System.gc ();
            }
        }

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
