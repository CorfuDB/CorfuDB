package org.corfudb.test.managedtable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.test.PojoSerializer;
import org.corfudb.test.StringIndexer;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.rocksdb.Options;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.UUID;

public class ManagedCorfuTableSetupManager<K extends Message, V extends Message, M extends Message> {

    private final ManagedCorfuTableSetup<K, V, M> persistedCorfu = config -> {
        String diskBackedDirectory = "/tmp/";

        Options defaultOptions = new Options().setCreateIfMissing(true);
        ISerializer defaultSerializer = new PojoSerializer(String.class);

        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, config.getTableName()));

        return config
                .getRt()
                .getObjectsView()
                .build()
                .setTypeToken(PersistedCorfuTable.<K, CorfuRecord<V, M>>getTypeToken())
                .setArguments(persistenceOptions.build(), defaultOptions, defaultSerializer, new StringIndexer())
                .setStreamName(config.getTableName())
                .setSerializer(defaultSerializer)
                .open();
    };

    private final ManagedCorfuTableSetup<K, V, M> persistentCorfu = config -> {
        String defaultInstanceMethodName = TableOptions.DEFAULT_INSTANCE_METHOD_NAME;
        K defaultKeyMessage = (K) config.getKClass().getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(config.getRt(), defaultKeyMessage);

        V defaultValueMessage = (V) config.getVClass().getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(config.getRt(), defaultValueMessage);

        M defaultMetadataMessage = (M) config.getMClass().getMethod(defaultInstanceMethodName).invoke(null);
        addTypeToClassMap(config.getRt(), defaultMetadataMessage);

        Object[] args = config.getArgs(defaultValueMessage);

        ProtobufSerializer serializer = getProtobufSerializer(config.getRt());

        PersistentCorfuTable<K, CorfuRecord<V, M>> table = new PersistentCorfuTable<>();

        String fullyQualifiedTableName = config.getFullyQualifiedTableName();
        MVOCorfuCompileProxy proxy = new MVOCorfuCompileProxy(
                config.getRt(),
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                table.getTableTypeToken().getRawType(),
                args,
                serializer,
                new HashSet<UUID>(),
                table,
                ObjectOpenOption.CACHE,
                config.getRt().getObjectsView().getMvoCache()
        );

        table.setCorfuSMRProxy(proxy);

        return table;
    };

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
                ManagedCorfuTableConfig<K, V, M> config
        ) throws Exception;

        default ManagedCorfuTableSetup<K, V, M> withToString(String toString) {
            return new ManagedCorfuTableSetup<K, V, M>(){
                @Override
                public GenericCorfuTable<? extends SnapshotGenerator<?>, K, CorfuRecord<V, M>> setup(
                        ManagedCorfuTableConfig<K, V, M> config) throws Exception {
                    return ManagedCorfuTableSetup.this.setup(config);
                }

                public String toString(){
                    return toString;
                }
            };
        }
    }
}
