package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuOptions.PersistenceOptions;
import org.corfudb.runtime.CorfuOptions.SchemaOptions;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
@Builder(toBuilder = true)
public class TableOptions {
    public static final String DEFAULT_INSTANCE_METHOD_NAME = "getDefaultInstance";

    /**
     * @deprecated Please use {@link PersistenceOptions}.
     * If this path is set, {@link CorfuStore} will utilize disk-backed {@link ICorfuTable}.
     */
    @Deprecated
    private final Path persistentDataPath;

    @Getter
    @Builder.Default
    private final boolean secondaryIndexesDisabled = false;

    @Getter
    @Builder.Default
    private final PersistenceOptions persistenceOptions = PersistenceOptions.getDefaultInstance();

    /**
     * Capture options like stream tags, backup restore, log replication at Table level
     */
    @Getter
    private final SchemaOptions schemaOptions;

    public Optional<Path> getPersistentDataPath() {
        return Optional.ofNullable(persistentDataPath);
    }

    /**
     * Helper function to extract corfu table schema options from message
     * and also preserve existing options like persistedPath
     * @param vClass - the java class created from a .proto message definition
     * @param tableOptions - old table options to migrate from
     * @return TableOptions that carry the message options defined within the proto
     */
    public static <V extends Message> TableOptions fromProtoSchema(Class<V> vClass,
                                                                   TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        TableOptions.TableOptionsBuilder tableOptionsBuilder = TableOptions.builder();
        if (tableOptions != null) {
            tableOptionsBuilder = tableOptions.toBuilder();
        }

        // some test cases pass vClass as null to verify behavior
        if (vClass == null) {
            return tableOptionsBuilder.build();
        }

        V defaultValueMessage = (V) vClass.getMethod(TableOptions.DEFAULT_INSTANCE_METHOD_NAME).invoke(null);

        tableOptionsBuilder.schemaOptions(defaultValueMessage
                .getDescriptorForType()
                .getOptions()
                .getExtension(CorfuOptions.tableSchema));

        return tableOptionsBuilder.build();
    }

    public static <V extends Message> TableOptions fromProtoSchema(Class<V> vClass)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return fromProtoSchema(vClass, null);
    }
}