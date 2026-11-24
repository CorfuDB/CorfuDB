package org.corfudb.runtime;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TextFormat;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotSyncSchemaTest {
    @Test
    void leaseFieldsAndEnumsRemainCompatibleWithTheOriginalWireAndStorageSchema() throws Exception {
        DescriptorProto.Builder original = DescriptorProto.newBuilder();
        try (InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream(
                "/snapshot/lease-v1-descriptor.txt"), StandardCharsets.UTF_8)) {
            TextFormat.merge(reader, original);
        }
        assertEquals(original.build(), SnapshotSyncLeaseRecord.getDescriptor().toProto());
        assertEquals("org.corfudb.runtime.SnapshotSyncLeaseRecord", SnapshotSyncLeaseRecord.getDescriptor().getFullName());
        assertEquals(SnapshotSyncLeaseRecord.getDescriptor(), LogReplication.LogReplicationMetadataResponseMsg
                .getDescriptor().findFieldByNumber(11).getMessageType());
        assertEquals(SnapshotSyncLeaseRecord.getDescriptor(), LogReplication.LogReplicationBusyResponseMsg
                .getDescriptor().findFieldByNumber(2).getMessageType());
    }

    @Test
    void registeringDurableLeaseMetadataDoesNotRegisterTransportMessages() {
        assertEquals(CorfuCompactorManagement.StringKey.getDescriptor().getFile(),
                SnapshotSyncLeaseRecord.getDescriptor().getFile());
        Set<String> files = new HashSet<>();
        collectDependencies(SnapshotSyncLeaseRecord.getDescriptor().getFile(), files);
        collectDependencies(CorfuCompactorManagement.StringKey.getDescriptor().getFile(), files);
        assertFalse(files.contains("log_replication.proto"));
        assertFalse(files.contains("corfu_message.proto"));
    }

    private void collectDependencies(FileDescriptor file, Set<String> files) {
        if (files.add(file.getName())) {
            file.getDependencies().forEach(dependency -> collectDependencies(dependency, files));
        }
    }
}
