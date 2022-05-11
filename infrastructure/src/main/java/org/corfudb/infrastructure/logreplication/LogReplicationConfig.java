package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
@ToString
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the active cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    @Getter
    // This would be done by joining the registry table and the domainMap. For POC, hard-coding it: lM-ID -> streams to replicate to the LM.
    private Map<String, Set<String>> lmToStreamsToSend; // this to be formed by replicationManager=> join DomainToStreamsToSend and the domainMap.

    private Map<String, Set<String>> domainToStreamsToSend; // This to be formed from registry table.

    // Streaming tags on Sink/Standby (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For instance, RegistryTable (to avoid losing local opened tables on standby)
    private Set<UUID> mergeOnlyStreams = new HashSet<>();

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this(streamsToReplicate, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED, MAX_CACHE_NUM_ENTRIES);
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize, int cacheSize) {
        this.streamsToReplicate = streamsToReplicate;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
        this.lmToStreamsToSend = new HashMap<>();
        this.lmToStreamsToSend.put("116e4567-e89b-12d3-a456-111664440011", new HashSet<String>(){{
            add("LR-Test$Table_Directory_Group-1"); add("LR-Test$Table_Directory_Group-3");
            add("CorfuSystem$ProtobufDescriptorTable"); add("CorfuSystem$RegistryTable");}});
        this.lmToStreamsToSend.put("226e4567-e89b-12d3-a456-111664440022", new HashSet<String>(){{
            add("LR-Test$Table_Directory_Group-1"); add("LR-Test$Table_Directory_Group-2");
            add("CorfuSystem$ProtobufDescriptorTable"); add("CorfuSystem$RegistryTable");}});
        this.lmToStreamsToSend.put("336e4567-e89b-12d3-a456-111664440033", new HashSet<String>(){{
            add("LR-Test$Table_Directory_Group-3"); add("CorfuSystem$ProtobufDescriptorTable");
            add("CorfuSystem$RegistryTable");}});

        // will be built from registry table
        domainToStreamsToSend = new HashMap<String, Set<String>>();
        domainToStreamsToSend.put("Domain1", new HashSet<String>(){{ add("LR-Test$Table_Directory_Group-1"); }});
        domainToStreamsToSend.put("Domain2", new HashSet<String>(){{ add("LR-Test$Table_Directory_Group-2"); }});
        domainToStreamsToSend.put("Domain3", new HashSet<String>(){{ add("LR-Test$Table_Directory_Group-3"); }});
    }

    public void updateLMToStreamsMap(String domainName, Set<String> LMsAddedToDomain, boolean isAdded) {
        // an LM gets added in domain. Then the streams sent ot LM will change ->
        // will have to include the tables that are being sent in the new domain to the LM

        for(String lm : LMsAddedToDomain) {
            log.info("before domainChange, lm {} had streams {}", lm, lmToStreamsToSend.get(lm));
            if (isAdded) {
                if (!lmToStreamsToSend.containsKey(lm)) {
                    lmToStreamsToSend.put(lm, new HashSet<String>());
                }
                lmToStreamsToSend.get(lm).addAll(domainToStreamsToSend.get(domainName));
            } else {
                // considering only positive case now. Negative case is that the domina never contained the site that the client has now requested to remove.

                lmToStreamsToSend.get(lm).removeAll(domainToStreamsToSend.get(domainName));
            }
            log.info("after domainChange, lm {} has streams {}...expect to see table3", lm, lmToStreamsToSend.get(lm));
        }
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize, MAX_CACHE_NUM_ENTRIES);
    }

    public LogReplicationConfig(Set<String> streamsToReplicate, Map<UUID, List<UUID>> streamingTagsMap,
                                Set<UUID> mergeOnlyStreams, int maxNumMsgPerBatch, int maxMsgSize, int cacheSize) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize, cacheSize);
        this.dataStreamToTagsMap = streamingTagsMap;
        this.mergeOnlyStreams = mergeOnlyStreams;
    }
}