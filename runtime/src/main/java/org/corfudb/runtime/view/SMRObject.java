package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
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
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder.CorfuTableType;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig.SmrObjectConfigBuilder;
import org.corfudb.runtime.view.StreamsView.StreamId;
import org.corfudb.runtime.view.StreamsView.StreamName;
import org.corfudb.runtime.view.StreamsView.StreamName.StreamNameBuilder;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
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
public class SMRObject<T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> {

    @NonNull
    private final CorfuRuntime runtime;

    @NonNull
    private final SmrObjectConfig<T, S> smrConfig;


    public static <T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> Builder<T, S> builder() {
        return new Builder<>();
    }

    public static <T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> T
    open(CorfuRuntime rt, SmrObjectConfig<T, S> smrConfig) {
        return new SMRObject<>(rt, smrConfig).open();
    }

    private T open() {
        String msg = "ObjectBuilder: open Corfu stream {}";
        //SmrObjectConfig<T, S> smrConfig = smrConfig;

        StreamName streamName = smrConfig.streamName;
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
            return new CorfuCompileWrapperBuilder<T, S>()
                    .getWrapper(this);
        } catch (Exception ex) {
            var errMsg = "Runtime instrumentation no longer supported and no compiled class found for {}";
            log.error(errMsg, smrConfig.type, ex);
            throw new UnrecoverableCorfuError(ex);
        }
    }

    public static class Builder<T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> {

        private final SmrObjectConfigBuilder<T, S> configBuilder;
        private CorfuRuntime corfuRuntime;

        public Builder() {
            configBuilder = SmrObjectConfig.builder();
        }

        public Builder(CorfuRuntime corfuRuntime) {
            this(corfuRuntime, SmrObjectConfig.builder());
        }

        public Builder(CorfuRuntime corfuRuntime, SmrObjectConfigBuilder<T, S> configBuilder) {
            this.configBuilder = configBuilder;
            this.corfuRuntime = corfuRuntime;
        }

        public SMRObject.Builder<T, S> setCorfuRuntime(CorfuRuntime corfuRuntime) {
            this.corfuRuntime = corfuRuntime;
            return ClassUtils.cast(this);
        }

        public SMRObject.Builder<T, S> setType(Class<T> type) {
            configBuilder.type(type);
            return this;
        }

        public SMRObject.Builder<T, S> setTypeToken(TypeToken<T> typeToken) {
            return setType(ClassUtils.cast(typeToken.getRawType()));
        }

        public SMRObject.Builder<T, S> setArguments(Object ... arguments) {
            configBuilder.arguments(arguments);
            return this;
        }

        public SMRObject.Builder<T, S> setStreamName(String streamName) {
            configBuilder.streamName = StreamName.build(streamName);
            return this;
        }

        public SMRObject.Builder<T, S> setStreamID(UUID streamId) {
            configBuilder.streamName.withId(new StreamId(streamId));
            return this;
        }

        public SMRObject.Builder<T, S> setSerializer(ISerializer serializer) {
            configBuilder.serializer(serializer);
            return this;
        }

        public SMRObject.Builder<T, S> addOpenOption(ObjectOpenOption openOption) {
            configBuilder.openOption(openOption);
            return this;
        }

        public SMRObject.Builder<T, S> setStreamTags(Set<UUID> tags) {
            configBuilder.streamTags(new HashSet<>(tags));
            return this;
        }

        public SMRObject.Builder<T, S> setStreamTags(UUID... tags) {
            configBuilder.streamTags(new HashSet<>(Arrays.asList(tags)));
            return this;
        }

        public SMRObject<T, S> build() {
            var config = configBuilder.build();
            return new SMRObject<>(corfuRuntime, config);
        }

        public T open() {
            return build().open();
        }
    }

    @lombok.Builder
    @AllArgsConstructor
    @Getter
    public static class SmrObjectConfig<T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> {
        @NonNull
        private final Class<T> type;

        @NonNull
        @With
        private final StreamName streamName;

        @NonNull
        @Default
        private final ISerializer serializer = Serializers.getDefaultSerializer();

        @NonNull
        @Default
        private ObjectOpenOption openOption = ObjectOpenOption.CACHE;

        @NonNull
        @Default
        private final Object[] arguments = new Object[0];

        @NonNull
        @Default
        private final Set<UUID> streamTags = new HashSet<>();

        public ObjectID getObjectId() {
            return new ObjectID(streamName.getId().getId(), type);
        }

        public boolean isCacheDisabled() {
            return openOption == ObjectOpenOption.NO_CACHE;
        }

        public CorfuTableType getTableType() {
            return CorfuTableType.parse(type);
        }

        public Class<S> tableImplementationType() {
            switch (getTableType()) {
                case PERSISTENT:
                    return ClassUtils.cast(ImmutableCorfuTable.class);
                case PERSISTED:
                    return ClassUtils.cast(DiskBackedCorfuTable.class);
            }

            throw new IllegalStateException("Unknown table type");
        }

        public T newSmrTableInstance() throws InvocationTargetException, IllegalAccessException, InstantiationException {
            var rawInstance = ReflectionUtils.
                    findMatchingConstructor(type.getDeclaredConstructors(), new Object[0]);
            return ClassUtils.cast(rawInstance);
        }
    }
}
