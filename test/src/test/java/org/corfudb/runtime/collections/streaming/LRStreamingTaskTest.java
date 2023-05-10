package org.corfudb.runtime.collections.streaming;

import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.collections.streaming.LRStreamingTask.LR_MULTI_NAMESPACE_LOGICAL_STREAM_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class runs all tests from its superclass - StreamingTaskTest, with the task type as LRStreamingTask.
 */
public class LRStreamingTaskTest extends StreamingTaskTest {

    // Variables for the Data table
    private final String dataNamespace = "test_namespace";
    private final String dataTableName = "table";
    private final String dataStreamTag = "tag_1";
    private final UUID dataStreamTagId = TableRegistry.getStreamIdForStreamTag(dataNamespace, dataStreamTag);

    // Variables for the System table
    private final String systemNamespace = CORFU_SYSTEM_NAMESPACE;
    private final String systemTableName = REPLICATION_STATUS_TABLE_NAME;
    private final String systemStreamTag = LR_STATUS_STREAM_TAG;
    private final UUID systemStreamTagId = TableRegistry.getStreamIdForStreamTag(systemNamespace, systemStreamTag);

    @Override
    protected void taskTypeSpecificSetup() {
        TableRegistry registry = mock(TableRegistry.class);
        when(runtime.getTableRegistry()).thenReturn(registry);

        Table dataTable = mock(Table.class);
        when(registry.getTable(dataNamespace, dataTableName)).thenReturn(dataTable);
        when(dataTable.getStreamTags()).thenReturn(Collections.singleton(dataStreamTagId));

        Table systemTable = mock(Table.class);
        when(registry.getTable(systemNamespace, systemTableName)).thenReturn(systemTable);
        when(systemTable.getStreamTags()).thenReturn(Collections.singleton(systemStreamTagId));

        Map<String, List<String>> nsToTableNames = new HashMap<>();
        nsToTableNames.put(dataNamespace, Arrays.asList(dataTableName));
        nsToTableNames.put(systemNamespace, Arrays.asList(systemTableName));

        Map<String, String> nsToStreamTags = new HashMap<>();
        nsToStreamTags.put(dataNamespace, dataStreamTag);
        nsToStreamTags.put(systemNamespace, systemStreamTag);

        task = new LRStreamingTask(runtime, workers, nsToStreamTags, nsToTableNames, listener, Address.NON_ADDRESS,
            bufferSize);
    }

    @Override
    protected UUID getTaskStreamId() {
        return LR_MULTI_NAMESPACE_LOGICAL_STREAM_ID;
    }
}
