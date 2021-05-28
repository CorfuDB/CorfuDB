package org.corfudb.infrastructure.compaction;

import com.google.common.reflect.TypeToken;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.compaction.exceptions.CompactionException;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Supplier;

import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * A checkpointTaskRequest processor that reads committed addresses from local node and writes
 * checkpoint via chain replication protocol. It will report task status to the checkpoint status
 * table when it accepts a task, completes a task or hits an error.
 */
@Slf4j
public class CheckpointTaskProcessor {

    private final String diskModePath;
    private final CorfuRuntime runtime;
    private final ISerializer dynamicProtobufSerializer;
    private final MultiCheckpointWriter<CorfuTable<CorfuDynamicKey,
            CorfuDynamicRecord>> multiCheckpointWriter;

    public CheckpointTaskProcessor(String diskModePath, CorfuRuntime runtime) {
        this.diskModePath = diskModePath;
        this.runtime = runtime;
        this.dynamicProtobufSerializer = new DynamicProtobufSerializer(runtime);
        this.multiCheckpointWriter = new MultiCheckpointWriter<>();
    }

    public CheckpointTaskResponse executeCheckpointTask(
            CheckpointTaskRequest taskRequest) {
        log.trace("Start to execute a checkpoint task with request {}", taskRequest);
        try {
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = openTable(taskRequest);
            multiCheckpointWriter.clearAllMaps();
            multiCheckpointWriter.addMap(table);
            multiCheckpointWriter.appendCheckpoints(runtime, CompactionManager.COMPACTION_REDESIGN);
        } catch (Exception e) {
            log.warn("CheckpointTask {} failed with exception {}.", taskRequest, e);
            return CheckpointTaskResponse.builder()
                    .request(taskRequest)
                    .status(CheckpointTaskResponse.Status.FAILED)
                    .causeOfFailure(Optional.of(new CompactionException(e)))
                    .build();
        }

        return CheckpointTaskResponse.builder()
                .request(taskRequest)
                .status(CheckpointTaskResponse.Status.FINISHED)
                .build();
    }


    private CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> openTable(CheckpointTaskRequest taskRequest) {
        String tableName = taskRequest.getTableName();
        String namespace = taskRequest.getNamespace();
        String streamName = getFullyQualifiedTableName(namespace, tableName);

        log.trace("Opening table {} in namespace {}. Options: {}", tableName, namespace, taskRequest.getOptions());

        SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> builder =
                runtime.getObjectsView().build()
                        .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
                        })
                        .setStreamName(streamName)
                        .setSerializer(dynamicProtobufSerializer)
                        .addOpenOption(ObjectOpenOption.NO_CACHE);

        if (taskRequest.getOptions().isDiskBacked()) {
            if (diskModePath == null || diskModePath.equals("")) {
                throw new IllegalArgumentException("diskModePath is not configured.");
            }
            final String diskModePathDirName = String.format("compactor_%s_%s", namespace, tableName);
            final Path persistedCacheLocation = Paths.get(diskModePath).resolve(diskModePathDirName);
            final Supplier<StreamingMap<CorfuDynamicKey, CorfuDynamicRecord>> mapSupplier = () -> new PersistedStreamingMap<>(
                    persistedCacheLocation, PersistedStreamingMap.getPersistedStreamingMapOptions(),
                    dynamicProtobufSerializer, runtime);
            builder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }

        return builder.open();
    }
}
