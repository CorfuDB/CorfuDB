package org.corfudb.test.managedtable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable.TableDescriptors;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public interface ManagedCorfuTableConfig<K, V> {
    void configure(CorfuRuntime rt) throws Exception;
    ISerializer getSerializer(CorfuRuntime rt);
    String getFullyQualifiedTableName();
    String getTableName();

    @Builder
    @Getter
    class ManagedCorfuTableProtobufConfig<K extends Message, V extends Message, M extends Message>
            implements ManagedCorfuTableConfig<K, V> {
        @NonNull
        private final TableRegistry.TableDescriptor<K, V, M> tableDescriptor;

        @Default
        private final String namespace = "some-namespace";

        @Default
        private final String tableName = "some-table";

        @Default
        private final boolean withSchema = true;

        public static ManagedCorfuTableConfig<Uuid, Uuid> buildUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Uuid, Uuid>builder()
                    .tableDescriptor(TableDescriptors.UUID)
                    .build();
        }

        public static ManagedCorfuTableProtobufConfig<Uuid, ExampleValue, ManagedMetadata> buildExampleVal(
                Consumer<ManagedCorfuTableProtobufConfigBuilder<Uuid, ExampleValue, ManagedMetadata>> customizer) {

            ManagedCorfuTableProtobufConfigBuilder<Uuid, ExampleValue, ManagedMetadata> builder =
                    ManagedCorfuTableProtobufConfig
                            .<Uuid, ExampleValue, ManagedMetadata>builder()
                            .tableDescriptor(TableDescriptors.EXAMPLE_VALUE);

            customizer.accept(builder);

            return builder.build();
        }

        @Override
        public void configure(CorfuRuntime rt) throws Exception {
            setupSerializer(rt);

            ProtobufSerializer serializer = getSerializer(rt);
            K defaultKeyMessage = tableDescriptor.getDefaultKeyMessage();
            addTypeToClassMap(serializer, defaultKeyMessage);

            V defaultValueMessage = tableDescriptor.getDefaultValueMessage();
            addTypeToClassMap(serializer, defaultValueMessage);

            M defaultMetadataMessage = tableDescriptor.getDefaultMetadataMessage();
            addTypeToClassMap(serializer, defaultMetadataMessage);
        }

        /**
         * Register a Protobuf serializer with the default runtime.
         */
        private void setupSerializer(CorfuRuntime rt) {
            setupSerializer(rt, new ProtobufSerializer(new ConcurrentHashMap<>()));
        }

        /**
         * Register a giver serializer with a given runtime.
         */
        protected void setupSerializer(@Nonnull final CorfuRuntime runtime, @Nonnull final ISerializer serializer) {
            runtime.getSerializers().registerSerializer(serializer);
        }

        @Override
        public ProtobufSerializer getSerializer(CorfuRuntime rt) {
            return rt
                    .getSerializers()
                    .getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE, ProtobufSerializer.class);
        }

        Object[] getArgs() throws Exception {
            if (withSchema) {
                return new Object[]{getProtobufIndexer()};
            } else {
                return new Object[]{};
            }
        }

        public ProtobufIndexer getProtobufIndexer() throws Exception {
            SchemaOptions schemaOptions = tableDescriptor.getSchemaOptions();
            V msg = tableDescriptor.getDefaultValueMessage();
            return new ProtobufIndexer(msg, schemaOptions);
        }

        /**
         * Fully qualified table name created to produce the stream uuid.
         */
        @Override
        public String getFullyQualifiedTableName() {
            return namespace + "$" + tableName;
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        /**
         * Adds the schema to the class map to enable serialization of this table data.
         */
        private <T extends Message> void addTypeToClassMap(ProtobufSerializer serializer, T msg) {
            String typeUrl = getTypeUrl(msg.getDescriptorForType());
            serializer.getClassMap().put(typeUrl, msg.getClass());
        }

        /**
         * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
         * Note: This is same as used in Any.proto.
         */
        private String getTypeUrl(Descriptor descriptor) {
            return "type.googleapis.com/" + descriptor.getFullName();
        }
    }
}