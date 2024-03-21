package org.corfudb.util;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.SneakyThrows;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.ImmutableCorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.SafeProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;

public class TableHelper {
    public enum TableType {
        PERSISTENT_TABLE,
        PERSISTED_TABLE,
    }

    @SneakyThrows
    public static <K, V> ICorfuTable<K, V> openTablePlain(String tableName, CorfuRuntime runtime, TableType tableType) {
        if (tableType == TableType.PERSISTENT_TABLE) {
            return runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<PersistentCorfuTable<K, V>>() {})
                    .setStreamName(tableName)
                    .open();
        } else {
            Path tempDir = Files.createTempDirectory("myTempDirPrefix");
            PersistenceOptions.PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                    .dataPath(tempDir);
            return runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<PersistedCorfuTable<K, V>>() {})
                    .setStreamName(tableName)
                    .setArguments(persistenceOptions.build(), DiskBackedCorfuTable.defaultOptions, Serializers.JSON)
                    .open();
        }
    }

    @SneakyThrows
    public static <K, V> ICorfuTable<K, V> openTablePlain(String tableName, CorfuRuntime runtime, ISerializer serializer, TableType tableType) {
        if (tableType == TableType.PERSISTENT_TABLE) {
            return runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<PersistentCorfuTable<K, V>>() {})
                    .setStreamName(tableName)
                    .setSerializer(serializer)
                    .open();
        } else {
            Path tempDir = Files.createTempDirectory("myTempDirPrefix");
            PersistenceOptions.PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                    .dataPath(tempDir);
            return runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<PersistedCorfuTable<K, V>>() {})
                    .setStreamName(tableName)
                    .setSerializer(serializer)
                    .setArguments(persistenceOptions.build(), DiskBackedCorfuTable.defaultOptions, serializer)
                    .open();
        }
    }

    public static ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>>
    openTable(TableType tableType, CorfuRuntime runtime, String namespace, String tableName) {
        if (tableType == TableType.PERSISTENT_TABLE) {
            return openTable(
                    runtime,
                    namespace,
                    tableName,
                    TestSchema.Uuid.class,
                    TestSchema.Uuid.class,
                    TestSchema.Uuid.class,
                    null
            );
        } else {
            return openDiskBackedTable(
                    runtime,
                    namespace,
                    tableName,
                    TestSchema.Uuid.class,
                    TestSchema.Uuid.class,
                    TestSchema.Uuid.class,
                    null
            );
        }
    }

    @SneakyThrows
    public static <K extends Message, V extends Message, M extends Message>
    ICorfuTable<K, CorfuRecord<V, M>> openTable(
            TableType tableType,
            @Nonnull final CorfuRuntime runtime,
            @Nonnull final String namespace,
            @Nonnull final String tableName,
            @Nonnull final Class<K> kClass,
            @Nonnull final Class<V> vClass,
            @Nullable final Class<M> mClass,
            @Nullable final CorfuOptions.SchemaOptions schemaOptions) {
        if (tableType == TableType.PERSISTENT_TABLE) {
            return openTable(
                    runtime,
                    namespace,
                    tableName,
                    kClass,
                    vClass,
                    mClass,
                    schemaOptions
            );
        } else {
            return openDiskBackedTable(
                    runtime,
                    namespace,
                    tableName,
                    kClass,
                    vClass,
                    mClass,
                    schemaOptions
            );
        }
    }

    @SneakyThrows
    private static <K extends Message, V extends Message, M extends Message>
    PersistentCorfuTable<K, CorfuRecord<V, M>> openTable(@Nonnull final CorfuRuntime runtime,
                                                         @Nonnull final String namespace,
                                                         @Nonnull final String tableName,
                                                         @Nonnull final Class<K> kClass,
                                                         @Nonnull final Class<V> vClass,
                                                         @Nullable final Class<M> mClass,
                                                         @Nullable final CorfuOptions.SchemaOptions schemaOptions) {

        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultValueMessage);

        if (mClass != null) {
            M defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);
            addTypeToClassMap(runtime, defaultMetadataMessage);
        }

        final String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        PersistentCorfuTable<K, CorfuRecord<V, M>> table = new PersistentCorfuTable<>();

        Object[] args = {};

        // If no schema options are provided, omit secondary indexes.
        if (schemaOptions != null) {
            args = new Object[]{new ProtobufIndexer(defaultValueMessage, schemaOptions)};
        }

        table.setCorfuSMRProxy(new MVOCorfuCompileProxy(
                runtime,
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                ImmutableCorfuTable.<K, CorfuRecord<V, M>>getTypeToken().getRawType(),
                PersistentCorfuTable.class,
                args,
                runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE),
                new HashSet<UUID>(),
                table,
                ObjectOpenOption.CACHE,
                runtime.getObjectsView().getMvoCache()
        ));

        return table;
    }

    private static String getTypeUrl(Descriptors.Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }
    private static <T extends Message> void addTypeToClassMap(@Nonnull final CorfuRuntime runtime, T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        ((ProtobufSerializer)runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE))
                .getClassMap().put(typeUrl, msg.getClass());
    }

    private static String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    @SneakyThrows
    private static <K extends Message, V extends Message, M extends Message>
    PersistedCorfuTable<K, CorfuRecord<V, M>> openDiskBackedTable(
            @Nonnull final CorfuRuntime runtime, @Nonnull final String namespace,
            @Nonnull final String tableName, @Nonnull final Class<K> kClass,
            @Nonnull final Class<V> vClass, @Nullable final Class<M> mClass,
            @Nullable final CorfuOptions.SchemaOptions schemaOptions) {

        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultValueMessage);

        if (mClass != null) {
            M defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);
            addTypeToClassMap(runtime, defaultMetadataMessage);
        }

        final String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        PersistedCorfuTable<K, CorfuRecord<V, M>> table = new PersistedCorfuTable<>();

        ArrayList<Object> args = new ArrayList<>();

        Path tempDir = Files.createTempDirectory("myTempDirPrefix");
        PersistenceOptions.PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(tempDir);

        ISerializer serializer = new SafeProtobufSerializer(runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE));

        args.add(persistenceOptions.build());
        args.add(DiskBackedCorfuTable.defaultOptions);
        args.add(serializer);
        // If no schema options are provided, omit secondary indexes.
        if (schemaOptions != null) {
            args.add(new ProtobufIndexer(defaultValueMessage, schemaOptions));
        }

        table.setCorfuSMRProxy(new MVOCorfuCompileProxy(
                runtime,
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                DiskBackedCorfuTable.<K, CorfuRecord<V, M>>getTypeToken().getRawType(),
                PersistedCorfuTable.class,
                args.toArray(),
                serializer,
                new HashSet<UUID>(),
                table,
                ObjectOpenOption.CACHE,
                runtime.getObjectsView().getMvoCache()
        ));

        return table;
    }

    @SneakyThrows
    public
    static <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(
            TableType tableType,
            CorfuStore corfuStore,
            String tableName, String namespace,
            Class<K> kClass,
            Class<V> vClass,
            Class<M> mClass,
            TableOptions tableOptions) {

        TableOptions newTableOption = tableOptions;
        if (tableType == TableType.PERSISTED_TABLE) {
            Path tempDir = Files.createTempDirectory("myTempDirPrefix");
            org.corfudb.runtime.CorfuOptions.PersistenceOptions persistenceOptions =
                    org.corfudb.runtime.CorfuOptions.PersistenceOptions.newBuilder()
                            .setDataPath(tempDir.toAbsolutePath().toString())
                            .build();
            newTableOption = tableOptions.toBuilder().persistenceOptions(persistenceOptions).build();
        }

        return corfuStore.openTable(
                namespace,
                tableName,
                kClass,
                vClass,
                mClass,
                newTableOption);
    }

}
