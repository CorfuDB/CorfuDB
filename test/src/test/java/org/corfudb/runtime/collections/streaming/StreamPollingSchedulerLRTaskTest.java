package org.corfudb.runtime.collections.streaming;

import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class runs all tests from its superclass - StreamPollingSchedulerTest, with the task type as LRStreamingTask.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class StreamPollingSchedulerLRTaskTest extends StreamPollingSchedulerTest {

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

    private final Map<String, List<String>> nsToTableNames = new HashMap<>();
    private final Map<String, String> nsToStreamTags = new HashMap<>();

    @Override
    @Test
    public void testShutdown() {
        // Do nothing.  This is a generic test in the super class and need not be run again.
    }

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

        nsToTableNames.put(dataNamespace, Arrays.asList(dataTableName));
        nsToTableNames.put(systemNamespace, Arrays.asList(systemTableName));

        nsToStreamTags.put(dataNamespace, dataStreamTag);
        nsToStreamTags.put(systemNamespace, systemStreamTag);
    }

    @Override
    protected void addTask(long lastAddress, int bufferSize) {
        streamPoller.addLRTask(listener, nsToStreamTags, nsToTableNames, lastAddress, bufferSize);
    }

    @Override
    protected Map<UUID, StreamAddressSpace> constructMockAddressMap(long lastAddressRead, boolean trim) {
        StreamAddressSpace dataSas = new StreamAddressSpace();
        StreamAddressSpace systemSas = new StreamAddressSpace();

        if (trim) {
            dataSas.setTrimMark(lastAddressRead+5);
            systemSas.setTrimMark(lastAddressRead+10);
        } else {
            dataSas.addAddress(lastAddressRead+1);
            addressesInStreamAddressSpace.add(lastAddressRead+1);

            systemSas.addAddress(lastAddressRead+2);
            addressesInStreamAddressSpace.add(lastAddressRead+2);
        }
        Map<UUID, StreamAddressSpace> map = new HashMap<>();
        map.put(dataStreamTagId, dataSas);
        map.put(systemStreamTagId, systemSas);
        return map;
    }

    @Override
    protected List<StreamAddressRange> constructRangeQueryList(long end) {
        StreamAddressRange dataRange = new StreamAddressRange(dataStreamTagId, Address.MAX, end);
        StreamAddressRange systemRange = new StreamAddressRange(systemStreamTagId, Address.MAX, end);
        return Arrays.asList(dataRange, systemRange);
    }

    // Constructs the next address after 'end'
    @Override
    protected Map<UUID, StreamAddressSpace> constructNextAddress(long end) {
        StreamAddressSpace dataSas = new StreamAddressSpace();
        StreamAddressSpace systemSas = new StreamAddressSpace();
        dataSas.addAddress(end+1);
        systemSas.addAddress(end+2);
        Map<UUID, StreamAddressSpace> map = new HashMap<>();
        map.put(dataStreamTagId, dataSas);
        map.put(systemStreamTagId, systemSas);
        return map;
    }
}
