package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.ImmutableCorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder.CorfuTableType;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig.SmrObjectConfigBuilder;
import org.corfudb.runtime.view.SMRObject.SmrTableConfig.SmrTableConfigBuilder;
import org.corfudb.runtime.view.StreamsView.StreamId;
import org.corfudb.runtime.view.StreamsView.StreamName;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 *
 * This class provides a builder that takes an SMR object definition with some options, wraps it with
 * a proxy and returns an object reference consumable through the ObjectsView (i.e. becomes replicated and
 * transactional)
 */
@Slf4j
@Getter
@AllArgsConstructor
public class SMRObject<T extends ICorfuSMR<?>> {

    @NonNull
    private final CorfuRuntime runtime;

    @NonNull
    private final SmrObjectConfig<T> smrConfig;

    public static <T extends ICorfuSMR<?>> Builder<T> builder() {
        return new Builder<>();
    }

    public static <T extends ICorfuSMR<?>> T open(CorfuRuntime rt, SmrObjectConfig<T> smrConfig) {
        return new SMRObject<>(rt, smrConfig).open();
    }

    private T open() {
        String msg = "ObjectBuilder: open Corfu stream {}";

        StreamName streamName = smrConfig.tableConfig.streamName;
        log.info(CorfuRuntime.LOG_NOT_IMPORTANT, msg, streamName);

        var oid = smrConfig.getObjectId();

        final T result = getWrapper();

        if (smrConfig.isCacheDisabled()) {
            return result;
        }

        Function<ObjectID, T> tableFactory = x -> {
            // Get object serializer to check if we didn't attempt to set another serializer
            // to an already existing map
            ISerializer objectSerializer = result.getCorfuSMRProxy().getSerializer();
            if (smrConfig.getSerializer() != objectSerializer) {
                log.warn("open: Attempt to open an existing object with a different serializer {}. " +
                                "Object {} opened with original serializer {}.",
                        smrConfig.getSerializer().getClass().getSimpleName(),
                        oid,
                        objectSerializer.getClass().getSimpleName()
                );
            }
            log.info("Added SMRObject {} to objectCache", oid);
            return result;
        };

        ICorfuSMR<?> obj = getRuntime()
                .getObjectsView()
                .getObjectCache()
                .computeIfAbsent(oid, tableFactory);

        return ClassUtils.cast(obj);
    }

    private T getWrapper() {
        try {
            return new CorfuCompileWrapperBuilder<T>()
                    .getWrapper(this);
        } catch (Exception ex) {
            var errMsg = "Runtime instrumentation no longer supported and no compiled class found for {}";
            log.error(errMsg, smrConfig.type, ex);
            throw new UnrecoverableCorfuError(ex);
        }
    }

    public static class Builder<T extends ICorfuSMR<?>> {

        private final SmrObjectConfigBuilder<T> configBuilder;
        private final SmrTableConfigBuilder tableConfigBuilder;

        private CorfuRuntime corfuRuntime;

        public Builder() {
            configBuilder = SmrObjectConfig.builder();
            tableConfigBuilder = SmrTableConfig.builder();
        }

        public Builder(CorfuRuntime corfuRuntime) {
            this(corfuRuntime, SmrObjectConfig.builder(), SmrTableConfig.builder());
        }

        public Builder(CorfuRuntime corfuRuntime, SmrObjectConfigBuilder<T> configBuilder, SmrTableConfigBuilder tableConfigBuilder) {
            this.configBuilder = configBuilder;
            this.tableConfigBuilder = tableConfigBuilder;
            this.corfuRuntime = corfuRuntime;
        }

        public SMRObject.Builder<T> setCorfuRuntime(CorfuRuntime corfuRuntime) {
            this.corfuRuntime = corfuRuntime;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setTypeToken(TypeToken<R> typeToken) {
            configBuilder.type((TypeToken<T>) typeToken);
            return (SMRObject.Builder<R>) this;
        }

        public SMRObject.Builder<T> setArguments(Object ... arguments) {
            tableConfigBuilder.arguments(arguments);
            return this;
        }

        public SMRObject.Builder<T> setStreamName(String streamName) {
            tableConfigBuilder.streamName(StreamName.build(streamName));
            return this;
        }

        public SMRObject.Builder<T> setStreamID(UUID streamId) {
            if (tableConfigBuilder.streamName == null){
                var streamName = new StreamName(new StreamId(streamId), Optional.empty());
                tableConfigBuilder.streamName(streamName);
            } else {
                tableConfigBuilder.streamName.withId(new StreamId(streamId));
            }
            return this;
        }

        public SMRObject.Builder<T> setSerializer(ISerializer serializer) {
            configBuilder.serializer(serializer);
            return this;
        }

        public SMRObject.Builder<T> addOpenOption(ObjectOpenOption openOption) {
            tableConfigBuilder.openOption(openOption);
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(Set<UUID> tags) {
            tableConfigBuilder.streamTags(tags);
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(UUID... tags) {
            tableConfigBuilder.streamTags(new HashSet<>(Arrays.asList(tags)));
            return this;
        }

        public SMRObject<T> build() {
            var tableConfig = tableConfigBuilder.build();
            var config = configBuilder
                    .tableConfig(tableConfig)
                    .build();
            return new SMRObject<>(corfuRuntime, config);
        }

        public T open() {
            return build().open();
        }
    }

    @lombok.Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class SmrTableConfig {
        @NonNull
        @With
        private final StreamName streamName;

        @NonNull
        @Default
        private ObjectOpenOption openOption = ObjectOpenOption.CACHE;

        @NonNull
        @Default
        private final Object[] arguments = new Object[0];

        @NonNull
        @Singular
        private final Set<UUID> streamTags;
    }

    @lombok.Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class SmrObjectConfig<T extends ICorfuSMR<?>> {
        @NonNull
        private final TypeToken<T> type;

        @NonNull
        @Default
        private final ISerializer serializer = Serializers.getDefaultSerializer();

        @NonNull
        private final SmrTableConfig tableConfig;

        public ObjectID getObjectId() {
            return new ObjectID(tableConfig.streamName.getId().getId(), type.getRawType());
        }

        public boolean isCacheDisabled() {
            return tableConfig.openOption == ObjectOpenOption.NO_CACHE;
        }

        public CorfuTableType getTableType() {
            return CorfuTableType.parse(type);
        }

        public <S extends SnapshotGenerator<S>> Class<S> tableImplementationType() {
            switch (getTableType()) {
                case PERSISTENT:
                    return ClassUtils.cast(ImmutableCorfuTable.class);
                case PERSISTED:
                    return ClassUtils.cast(DiskBackedCorfuTable.class);
            }

            throw new IllegalStateException("Unknown table type");
        }

        public T newSmrTableInstance() throws InvocationTargetException, IllegalAccessException, InstantiationException {
            Constructor<?>[] constructors = type.getRawType().getDeclaredConstructors();
            var rawInstance = ReflectionUtils.findMatchingConstructor(constructors, new Object[0]);
            return ClassUtils.cast(rawInstance);
        }
    }
}

