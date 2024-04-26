package org.corfudb.test.managedtable;

import lombok.Builder;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableConfigParams;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableProtobufConfig;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.UUID;

@Builder
public class ManagedCorfuTableSetupManager<K, V> {

    private final ManagedCorfuTableSetup<K, V> persistentPlainCorfu = (rt, config) -> rt.getObjectsView()
            .build()
            .setStreamName(config.getTableName().toFqdn())
            .setTypeToken(PersistentCorfuTable.<K, V>getTypeToken())
            .setSerializer(config.getSerializer(rt))
            .open();

    private final ManagedCorfuTableSetup<K, V> persistedPlainCorfu = (rt, config) -> rt.getObjectsView()
            .build()
            .setStreamName(config.getTableName().toFqdn())
            .setTypeToken(PersistedCorfuTable.<K, V>getTypeToken())
            .setSerializer(config.getSerializer(rt))
            .open();

    private final ManagedCorfuTableSetup<K, V> persistedProtobufCorfu = new ManagedCorfuTableSetup<>() {
        @Override
        public GenericCorfuTable<? extends SnapshotGenerator<?>, K, V> open(
                CorfuRuntime rt, ManagedCorfuTableConfig<K, V> config) throws Exception {
            config.configure(rt);

            String diskBackedDirectory = "/tmp/";
            Path dataPath = Paths.get(diskBackedDirectory, config.getTableName().toFqdn());
            PersistenceOptionsBuilder persistenceOptions = PersistenceOptions
                    .builder()
                    .dataPath(dataPath);

            Options defaultOptions = new Options().setCreateIfMissing(true);
            ProtobufIndexer indexer = ((ManagedCorfuTableProtobufConfig<?, ?, ?>) config).getIndexer();
            ISerializer serializer = config.getSerializer(rt);

            var smrCfg = SmrObjectConfig
                    .<PersistedCorfuTable<K, V>>builder()
                    .type(PersistedCorfuTable.getTypeToken())
                    .streamName(config.getTableName().toStreamName())
                    .serializer(serializer)
                    .arguments(new Object[]{persistenceOptions.build(), defaultOptions, serializer, indexer})
                    .build();

            return rt.getObjectsView().open(smrCfg);
        }

        @Override
        public String toString() {
            return "PersistedCT";
        }
    };

    private final ManagedCorfuTableSetup<K, V> persistentProtobufCorfu = new ManagedCorfuTableSetup<>() {
        @Override
        public GenericCorfuTable<?, K, V> open(CorfuRuntime rt, ManagedCorfuTableConfig<K, V> config) throws Exception {
            rt.getSerializers().registerSerializer(config.getSerializer(rt));

            switch (config.getParams().getTableType()) {
                case PERSISTENT:
                    return rt.getObjectsView().open(config.persistentCfg());
                    break;
                case PERSISTED:
                    return rt.getObjectsView().open(config.persistedCfg());
                    break;
            }

            throw new IllegalStateException("Unknown table type");
        }

        @Override
        public String toString() {
            return "PersistentCT";
        }
    };

    public static <K, V> ManagedCorfuTableSetup<K, V> getTableSetup(ManagedCorfuTableConfigParams params) {
        switch (params.getTableType()) {
            case PERSISTENT:
                switch (params.getSetupType()) {
                    case PLAIN_TABLE:
                        return persistentPlainCorfu();
                    case PROTOBUF_TABLE:
                        return persistentProtobufCorfu();
                }
                break;
            case PERSISTED:
                switch (params.getSetupType()) {
                    case PLAIN_TABLE:
                        return persistedPlainCorfu();
                    case PROTOBUF_TABLE:
                        return persistedProtobufCorfu();
                }
                break;
        }

        throw new IllegalStateException("Unknown configuration");
    }

    private static <K, V> ManagedCorfuTableSetup<K, V> persistentProtobufCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistentProtobufCorfu;
    }

    private static <K, V> ManagedCorfuTableSetup<K, V> persistedProtobufCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistedProtobufCorfu;
    }

    private static <K, V> ManagedCorfuTableSetup<K, V> persistedPlainCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistedPlainCorfu;
    }

    private static <K, V> ManagedCorfuTableSetup<K, V> persistentPlainCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistentPlainCorfu;
    }

    public interface ManagedCorfuTableSetup<K, V> {
        GenericCorfuTable<? extends SnapshotGenerator<?>, K, V> open(
                CorfuRuntime rt, ManagedCorfuTableConfig<K, V> config
        ) throws Exception;
    }

    public enum ManagedCorfuTableSetupType {
        PLAIN_TABLE, PROTOBUF_TABLE,
    }
}
