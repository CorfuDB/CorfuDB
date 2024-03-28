package org.corfudb.test.managedtable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import lombok.Builder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.rocksdb.Options;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.UUID;

@Builder
public class ManagedCorfuTableSetupManager<K extends Message, V extends Message, M extends Message> {

    private final ManagedCorfuTableSetup<K, V, M> persistedCorfu = (rt, config) -> {
        String diskBackedDirectory = "/tmp/";

        Options defaultOptions = new Options().setCreateIfMissing(true);

        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, config.getTableName()));

        setupTypes(rt, config);
        ProtobufIndexer indexer = config.getProtobufIndexer();
        ProtobufSerializer serializer = getProtobufSerializer(rt);

        return rt
                .getObjectsView()
                .build()
                .setTypeToken(PersistedCorfuTable.<K, CorfuRecord<V, M>>getTypeToken())
                .setArguments(persistenceOptions.build(), defaultOptions, serializer, indexer)
                .setStreamName(config.getTableName())
                .setSerializer(serializer)
                .open();
    };

    private final ManagedCorfuTableSetup<K, V, M> persistentCorfu = (rt, config) -> {
        setupTypes(rt, config);
        Object[] args = config.getArgs();
        ProtobufSerializer serializer = getProtobufSerializer(rt);

        PersistentCorfuTable<K, CorfuRecord<V, M>> table = new PersistentCorfuTable<>();

        String fullyQualifiedTableName = config.getFullyQualifiedTableName();
        MVOCorfuCompileProxy proxy = new MVOCorfuCompileProxy(
                rt,
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                table.getTableTypeToken().getRawType(),
                PersistentCorfuTable.class,
                args,
                serializer,
                new HashSet<UUID>(),
                table,
                ObjectOpenOption.CACHE,
                rt.getObjectsView().getMvoCache()
        );

        table.setCorfuSMRProxy(proxy);

        return table;
    };

    private void setupTypes(CorfuRuntime rt, ManagedCorfuTableConfig<K, V, M> config) throws Exception {
        TableDescriptor<K, V, M> descriptor = config.tableDescriptor();
        K defaultKeyMessage = descriptor.getDefaultKeyMessage();
        addTypeToClassMap(rt, defaultKeyMessage);

        V defaultValueMessage = descriptor.getDefaultValueMessage();
        addTypeToClassMap(rt, defaultValueMessage);

        M defaultMetadataMessage = descriptor.getDefaultMetadataMessage();
        addTypeToClassMap(rt, defaultMetadataMessage);
    }

    public static <K extends Message, V extends Message, M extends Message> ManagedCorfuTableSetup<K, V, M> persistentCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V, M>builder()
                .build()
                .getPersistentCorfu();
    }

    public static <K extends Message, V extends Message, M extends Message> ManagedCorfuTableSetup<K, V, M> persistedCorfu() {
        return ManagedCorfuTableSetupManager
                .<K, V, M>builder()
                .build()
                .getPersistedCorfu();
    }

    public ManagedCorfuTableSetup<K, V, M> getPersistentCorfu() {
        return persistentCorfu.withToString("PersistentCT");
    }

    public ManagedCorfuTableSetup<K, V, M> getPersistedCorfu() {
        return persistedCorfu.withToString("PersistedCT");
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     */
    private <T extends Message> void addTypeToClassMap(CorfuRuntime rt, T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        ProtobufSerializer serializer = getProtobufSerializer(rt);

        serializer.getClassMap().put(typeUrl, msg.getClass());
    }

    private ProtobufSerializer getProtobufSerializer(CorfuRuntime rt) {
        return rt
                .getSerializers()
                .getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE, ProtobufSerializer.class);
    }

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     */
    private String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    @FunctionalInterface
    public interface ManagedCorfuTableSetup<K extends Message, V extends Message, M extends Message> {
        GenericCorfuTable<? extends SnapshotGenerator<?>, K, CorfuRecord<V, M>> setup(
                CorfuRuntime rt, ManagedCorfuTableConfig<K, V, M> config
        ) throws Exception;

        default ManagedCorfuTableSetup<K, V, M> withToString(String toString) {
            return new ManagedCorfuTableSetup<>(){
                @Override
                public GenericCorfuTable<? extends SnapshotGenerator<?>, K, CorfuRecord<V, M>> setup(
                        CorfuRuntime rt, ManagedCorfuTableConfig<K, V, M> config) throws Exception {
                    return ManagedCorfuTableSetup.this.setup(rt, config);
                }

                public String toString(){
                    return toString;
                }
            };
        }
    }
}
