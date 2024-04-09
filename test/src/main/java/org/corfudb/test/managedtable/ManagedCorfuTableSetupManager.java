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

            Options defaultOptions = new Options().setCreateIfMissing(true);

            Path dataPath = Paths.get(diskBackedDirectory, config.getTableName().toFqdn());
            PersistenceOptionsBuilder persistenceOptions = PersistenceOptions
                    .builder()
                    .dataPath(dataPath);

            ProtobufIndexer indexer = ((ManagedCorfuTableProtobufConfig<?, ?, ?>) config).getIndexer();
            ISerializer serializer = config.getSerializer(rt);

            return rt
                    .getObjectsView()
                    .build()
                    .setTypeToken(PersistedCorfuTable.<K, V>getTypeToken())
                    .setArguments(persistenceOptions.build(), defaultOptions, serializer, indexer)
                    .setStreamName(config.getTableName().toFqdn())
                    .setSerializer(serializer)
                    .open();
        }

        @Override
        public String toString() {
            return "PersistedCT";
        }
    };

    private final ManagedCorfuTableSetup<K, V> persistentProtobufCorfu = new ManagedCorfuTableSetup<>() {
        @Override
        public GenericCorfuTable<? extends SnapshotGenerator<?>, K, V> open(
                CorfuRuntime rt, ManagedCorfuTableConfig<K, V> config) throws Exception {
            config.configure(rt);
            Object[] args = ((ManagedCorfuTableProtobufConfig<?, ?, ?>) config).getArgs();
            ISerializer serializer = config.getSerializer(rt);

            PersistentCorfuTable<K, V> table = new PersistentCorfuTable<>();

            MVOCorfuCompileProxy proxy = new MVOCorfuCompileProxy(
                    rt,
                    config.getTableName().toStreamId().getId(),
                    table.getTableTypeToken().getRawType(),
                    PersistentCorfuTable.class,
                    args,
                    serializer,
                    new HashSet<UUID>(),
                    table,
                    ObjectOpenOption.CACHE,
                    rt.getObjectsView().getMvoCache()
            );

            table.setCorfuSMRProxy(ClassUtils.cast(proxy));

            return table;
        }

        @Override
        public String toString() {
            return "PersistentCT";
        }
    };

    public static <K, V> ManagedCorfuTableSetup<K, V> persistentProtobufCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistentProtobufCorfu;
    }

    public static <K, V> ManagedCorfuTableSetup<K, V> persistedProtobufCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistedProtobufCorfu;
    }

    public static <K, V> ManagedCorfuTableSetup<K, V> persistedPlainCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V>builder()
                .build()
                .persistedPlainCorfu;
    }

    public static <K, V> ManagedCorfuTableSetup<K, V> persistentPlainCorfu() {
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
}
