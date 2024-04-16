package org.corfudb.test.managedtable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.Adult;
import org.corfudb.runtime.ExampleSchemas.Company;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Office;
import org.corfudb.runtime.ExampleSchemas.Person;
import org.corfudb.runtime.ExampleSchemas.SportsProfessional;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.CPSerializer;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableType;
import org.corfudb.test.managedtable.ManagedCorfuTable.TableDescriptors;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetupType;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public interface ManagedCorfuTableConfig {
    void configure(CorfuRuntime rt) throws Exception;
    ISerializer getSerializer(CorfuRuntime rt);
    FullyQualifiedTableName getTableName();
    ManagedCorfuTableConfigParams getParams();

    @Builder
    @Getter
    class ManagedCorfuTableGenericConfig implements ManagedCorfuTableConfig {
        @NonNull
        private final ManagedSerializer managedSerializer;
        @NonNull
        private final ManagedCorfuTableConfigParams params;

        @Default
        private final FullyQualifiedTableName tableName = FullyQualifiedTableName.builder()
                .namespace(Optional.of("some-namespace"))
                .tableName("some-table")
                .build();

        @Override
        public void configure(CorfuRuntime rt) throws Exception {
            managedSerializer.configure(rt);
        }

        @Override
        public ISerializer getSerializer(CorfuRuntime rt) {
            return managedSerializer.getSerializer();
        }

    }

    @Builder
    @Getter
    class ManagedCorfuTableProtobufConfig<K extends Message, V extends Message, M extends Message>
            implements ManagedCorfuTableConfig {
        @NonNull
        private final TableDescriptor<K, V, M> tableDescriptor;
        @NonNull
        private final ManagedCorfuTableConfigParams params;

        @Default
        private final FullyQualifiedTableName tableName = FullyQualifiedTableName.builder()
                .namespace(Optional.of("some-namespace"))
                .tableName("some-table")
                .build();

        @Default
        private final boolean withSchema = true;

        @Default
        private final ManagedSerializer managedSerializer = new ManagedProtobufSerializer();

        public static ManagedCorfuTableConfig buildUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Uuid, Uuid>builder()
                    .tableDescriptor(TableDescriptors.UUID)
                    .build();
        }

        public static ManagedCorfuTableConfig buildTestSchemaUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, ExampleValue, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.EXAMPLE_VALUE)
                    .build();
        }

        public static ManagedCorfuTableConfig buildExampleSchemaUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.EXAMPLE_SCHEMA_UUID_VALUE)
                    .build();
        }

        public static ManagedCorfuTableConfig buildCompanyAndUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Company, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.COMPANY)
                    .build();
        }

        public static ManagedCorfuTableConfig buildPerson() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Person, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.PERSON)
                    .build();
        }

        public static ManagedCorfuTableConfig buildOffice() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Office, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.OFFICE)
                    .build();
        }

        public static ManagedCorfuTableConfig buildAdult() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Adult, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.ADULT)
                    .build();
        }

        public static ManagedCorfuTableConfig buildSportsProfessional() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, SportsProfessional, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.SPORTS_PROFESSIONAL)
                    .build();
        }

        public static ManagedCorfuTableProtobufConfig<Uuid, ExampleValue, ManagedMetadata> buildExampleVal(
                Consumer<ManagedCorfuTableProtobufConfigBuilder<Uuid, ExampleValue, ManagedMetadata>> customizer) {

            var builder = ManagedCorfuTableProtobufConfig
                            .<Uuid, ExampleValue, ManagedMetadata>builder()
                            .tableDescriptor(TableDescriptors.EXAMPLE_VALUE);

            customizer.accept(builder);

            return builder.build();
        }

        @Override
        public void configure(CorfuRuntime rt) throws Exception {
            managedSerializer.configure(rt);

            ProtobufSerializer serializer = getSerializer(rt);
            K defaultKeyMessage = tableDescriptor.getDefaultKeyMessage();
            addTypeToClassMap(serializer, defaultKeyMessage);

            V defaultValueMessage = tableDescriptor.getDefaultValueMessage();
            addTypeToClassMap(serializer, defaultValueMessage);

            M defaultMetadataMessage = tableDescriptor.getDefaultMetadataMessage();
            addTypeToClassMap(serializer, defaultMetadataMessage);
        }

        @Override
        public ProtobufSerializer getSerializer(CorfuRuntime rt) {
            return rt
                    .getSerializers()
                    .getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE, ProtobufSerializer.class);
        }

        Object[] getArgs() throws Exception {
            if (withSchema) {
                return new Object[]{getIndexer()};
            } else {
                return new Object[]{};
            }
        }

        public ProtobufIndexer getIndexer() throws Exception {
            SchemaOptions schemaOptions = tableDescriptor.getSchemaOptions();
            V msg = tableDescriptor.getDefaultValueMessage();
            return new ProtobufIndexer(msg, schemaOptions);
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

    interface ManagedSerializer {
        void configure(CorfuRuntime rt);
        ISerializer getSerializer();
    }

    class ManagedProtobufSerializer implements ManagedSerializer {
        @Getter
        private ISerializer serializer;

        @Override
        public void configure(CorfuRuntime rt) {
            serializer = setupSerializer(rt);
        }

        /**
         * Register a Protobuf serializer with the default runtime.
         *
         * @return ProtobufSerializer
         */
        private ProtobufSerializer setupSerializer(CorfuRuntime rt) {
            ProtobufSerializer protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
            setupSerializer(rt, protoSerializer);
            return protoSerializer;
        }

        /**
         * Register a giver serializer with a given runtime.
         */
        protected void setupSerializer(@Nonnull final CorfuRuntime runtime, @Nonnull final ISerializer serializer) {
            runtime.getSerializers().registerSerializer(serializer);
        }
    }

    class ManagedDefaultSerializer implements ManagedSerializer {
        @Getter
        private ISerializer serializer;

        @Override
        public void configure(CorfuRuntime rt) {
            serializer = Serializers.getDefaultSerializer();
        }
    }

    class ManagedDynamicProtobufSerializer implements ManagedSerializer {
        @Getter
        private ISerializer serializer;

        @Override
        public void configure(CorfuRuntime rt) {
            //create and register dynamic serializer in the runtime
            serializer = new DynamicProtobufSerializer(rt);
        }
    }

    class ManagedCPSerializer implements ManagedSerializer {
        @Getter
        private ISerializer serializer;

        @Override
        public void configure(CorfuRuntime rt) {
            serializer = new CPSerializer();
            rt.getSerializers().registerSerializer(serializer);
        }
    }

    @AllArgsConstructor
    @Getter
    class ManagedCorfuTableConfigParams {
        public static final ManagedCorfuTableConfigParams PERSISTED_PROTOBUF_TABLE = new ManagedCorfuTableConfigParams(
                ManagedCorfuTableType.PERSISTED, ManagedCorfuTableSetupType.PROTOBUF_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTENT_PROTOBUF_TABLE = new ManagedCorfuTableConfigParams(
                ManagedCorfuTableType.PERSISTENT, ManagedCorfuTableSetupType.PROTOBUF_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTENT_PLAIN_TABLE = new ManagedCorfuTableConfigParams(
                ManagedCorfuTableType.PERSISTENT, ManagedCorfuTableSetupType.PLAIN_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTED_PLAIN_TABLE = new ManagedCorfuTableConfigParams(
                ManagedCorfuTableType.PERSISTED, ManagedCorfuTableSetupType.PLAIN_TABLE
        );

        private final ManagedCorfuTableType tableType;
        private final ManagedCorfuTableSetupType setupType;
    }
}