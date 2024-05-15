package org.corfudb.test.managedtable;

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
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.ProtobufIndexer;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder.CorfuTableType;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable.TableDescriptors;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager.ManagedCorfuTableSetupType;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.util.function.Consumer;

public interface ManagedCorfuTableConfig<K, V> {
    void configure(CorfuRuntime rt) throws Exception;

    ManagedCorfuTableConfigParams getParams();
    SmrObjectConfig<PersistentCorfuTable<K, V>> persistentCfg();
    SmrObjectConfig<PersistedCorfuTable<K, V>> persistedCfg();

    @Builder
    @Getter
    class ManagedCorfuTableProtobufConfig<K extends Message, V extends Message, M extends Message>
            implements ManagedCorfuTableConfig<K, V> {
        @NonNull
        private final TableDescriptor<K, V, M> tableDescriptor;
        @NonNull
        private final ManagedCorfuTableConfigParams params;

        @Default
        private final boolean withSchema = true;

        public SmrObjectConfig<PersistentCorfuTable<K, V>> persistentCfg() {
            var cfg = SmrObjectConfig
                    .<PersistentCorfuTable<K, V>>builder()
                    .type(PersistentCorfuTable.getTypeToken())
                    .streamName()
                    .serializer()
                    .build();
            return cfg;

        }

        @Override
        public SmrObjectConfig<PersistedCorfuTable<K, V>> persistedCfg() {
            var cfg = SmrObjectConfig
                    .<PersistedCorfuTable<K, V>>builder()
                    .type(PersistedCorfuTable.getTypeToken())
                    //.streamName()
                    .build();

            return cfg;
        }

        public static ManagedCorfuTableConfig<Uuid, Uuid> buildUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Uuid, Uuid>builder()
                    .tableDescriptor(TableDescriptors.UUID)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, ExampleValue> buildTestSchemaUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, ExampleValue, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.EXAMPLE_VALUE)
                    .build();
        }

        public static ManagedCorfuTableConfig<ExampleSchemas.Uuid, ExampleValue> buildExampleSchemaUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.EXAMPLE_SCHEMA_UUID_VALUE)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, Company> buildCompanyAndUuid() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Company, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.COMPANY)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, Person> buildPerson() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Person, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.PERSON)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, Office> buildOffice() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Office, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.OFFICE)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, Adult> buildAdult() {
            return ManagedCorfuTableProtobufConfig
                    .<Uuid, Adult, ManagedMetadata>builder()
                    .tableDescriptor(TableDescriptors.ADULT)
                    .build();
        }

        public static ManagedCorfuTableConfig<Uuid, SportsProfessional> buildSportsProfessional() {
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
            ProtobufSerializer serializer = getSerializer(rt);
            serializer.registerTypes(tableDescriptor);
            rt.getSerializers().registerSerializer();
        }

        @Override
        public ProtobufSerializer getSerializer(CorfuRuntime rt) {
            return rt
                    .getSerializers()
                    .getProtobufSerializer();
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
    }

    @AllArgsConstructor
    @Getter
    class ManagedCorfuTableConfigParams {
        public static final ManagedCorfuTableConfigParams PERSISTED_PROTOBUF_TABLE = new ManagedCorfuTableConfigParams(
                CorfuTableType.PERSISTED, ManagedCorfuTableSetupType.PROTOBUF_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTENT_PROTOBUF_TABLE = new ManagedCorfuTableConfigParams(
                CorfuTableType.PERSISTENT, ManagedCorfuTableSetupType.PROTOBUF_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTENT_PLAIN_TABLE = new ManagedCorfuTableConfigParams(
                CorfuTableType.PERSISTENT, ManagedCorfuTableSetupType.PLAIN_TABLE
        );

        public static final ManagedCorfuTableConfigParams PERSISTED_PLAIN_TABLE = new ManagedCorfuTableConfigParams(
                CorfuTableType.PERSISTED, ManagedCorfuTableSetupType.PLAIN_TABLE
        );

        private final CorfuTableType tableType;
        private final ManagedCorfuTableSetupType setupType;
    }
}