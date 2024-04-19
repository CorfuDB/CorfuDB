package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
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
public class SMRObject<T extends ICorfuSMR<?>> {

    @NonNull
    private final SmrObjectConfig<T, ?> smrConfig;

    @NonNull
    private final CorfuRuntime runtime;

    public static <T extends ICorfuSMR<?>> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T extends ICorfuSMR<?>> {

        private final SmrObjectConfigBuilder<T, ?> configBuilder = SmrObjectConfig.builder();
        private final StreamNameBuilder streamNameBuilder = StreamName.builder();
        private CorfuRuntime corfuRuntime;

        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setCorfuRuntime(CorfuRuntime corfuRuntime) {
            this.corfuRuntime = corfuRuntime;
            return ClassUtils.cast(this);
        }

        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setType(Class<R> type) {
            configBuilder.type(ClassUtils.cast(type));
            return ClassUtils.cast(this);
        }

        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setTypeToken(TypeToken<R> typeToken) {
            return setType(ClassUtils.cast(typeToken.getRawType()));
        }

        public SMRObject.Builder<T> setArguments(Object ... arguments) {
            configBuilder.arguments(arguments);
            return this;
        }

        public SMRObject.Builder<T> setStreamName(String streamName) {
            configBuilder.streamName = StreamName.build(streamName);
            return this;
        }

        public SMRObject.Builder<T> setStreamID(UUID streamId) {
            streamNameBuilder.id(new StreamId(streamId));
            return this;
        }

        public SMRObject.Builder<T> setSerializer(ISerializer serializer) {
            configBuilder.serializer(serializer);
            return this;
        }

        public SMRObject.Builder<T> addOpenOption(ObjectOpenOption openOption) {
            configBuilder.openOption(openOption);
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(Set<UUID> tags) {
            configBuilder.streamTags(new HashSet<>(tags));
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(UUID... tags) {
            configBuilder.streamTags(new HashSet<>(Arrays.asList(tags)));
            return this;
        }

        public SMRObject<T> build() {
            StreamName streamName = streamNameBuilder.build();
            configBuilder.streamName(streamName);
            var config = configBuilder.build();
            return new SMRObject<>(config, corfuRuntime);
        }

        public T open() {
            final SMRObject<T> smrObject = build();

            String msg = "ObjectBuilder: open Corfu stream {}";
            SmrObjectConfig<T, ?> smrConfig = smrObject.smrConfig;

            StreamName streamName = smrConfig.streamName;
            log.info(CorfuRuntime.LOG_NOT_IMPORTANT, msg, streamName);

            var oid = smrConfig.getObjectId();

            final T result = getWrapper(smrObject);

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

            ICorfuSMR<?> obj = smrObject
                    .getRuntime()
                    .getObjectsView()
                    .getObjectCache()
                    .computeIfAbsent(oid, tableFactory);

            return ClassUtils.cast(obj);
        }

        private T getWrapper(SMRObject<T> smrObject) {
            try {
                return CorfuCompileWrapperBuilder.getWrapper(smrObject);
            } catch (Exception ex) {
                var errMsg = "Runtime instrumentation no longer supported and no compiled class found for {}";
                log.error(errMsg, smrObject.smrConfig.type, ex);
                throw new UnrecoverableCorfuError(ex);
            }
        }
    }

    @lombok.Builder
    @AllArgsConstructor
    @Getter
    public static class SmrObjectConfig<T extends ICorfuSMR<S>, S extends SnapshotGenerator<S> & ConsistencyView> {
        @NonNull
        private final Class<T> type;

        @NonNull
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
