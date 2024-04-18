package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.StreamsView.StreamId;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 *
 * This class provides a builder that takes an SMR object definition with some options, wraps it with
 * a proxy and returns a object reference consumable through the ObjectsView (i.e. becomes replicated and
 * transactional)
 * <p>
 * Created by mwei on 4/6/16.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class SMRObject<T extends ICorfuSMR<?>> {

    @NonNull
    private final SmrObjectConfig<T> smrConfig;

    @NonNull
    private final CorfuRuntime runtime;

    @NonNull
    private final Class<T> type;

    @NonNull
    @SuppressWarnings("checkstyle:abbreviation")
    private final UUID streamID;

    private final String streamName;

    @NonNull
    private final ISerializer serializer;

    @NonNull
    private ObjectOpenOption openOption;

    @NonNull
    private final Object[] arguments;

    @NonNull
    private final Set<UUID> streamTags;

    @lombok.Builder
    @AllArgsConstructor
    public static class SmrObjectConfig<T extends ICorfuSMR<?>> {
        @NonNull
        private final Class<T> type;

        @NonNull
        private final UUID streamId;

        @NonNull
        private final String streamName;

        @NonNull
        @Default
        private final ISerializer serializer = Serializers.getDefaultSerializer();

        @NonNull
        private ObjectOpenOption openOption;

        @NonNull
        @Default
        private final Object[] arguments = new Object[0];

        @NonNull
        private final Set<UUID> streamTags;
    }

    public static class Builder<T extends ICorfuSMR<?>> {

        private String streamName;
        private ISerializer serializer = Serializers.getDefaultSerializer();
        private Object[] arguments = new Object[0];
        private ObjectOpenOption openOption = ObjectOpenOption.CACHE;
        private CorfuRuntime corfuRuntime;

        @Getter
        private Class<T> type;
        @Getter
        public UUID streamID;
        @Getter
        private Set<UUID> streamTags = new HashSet<>();

        private void verify() {
            if (this.streamName != null && !UUID.nameUUIDFromBytes(streamName.getBytes()).equals(streamID)) {
                throw new IllegalArgumentException("Stream id must be derived from stream name");
            }
        }
        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setCorfuRuntime(CorfuRuntime corfuRuntime) {
            this.corfuRuntime = corfuRuntime;
            return (SMRObject.Builder<R>) this;
        }

        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setType(Class<R> type) {
            this.type = ClassUtils.cast(type);
            return ClassUtils.cast(this);
        }

        public <R extends ICorfuSMR<?>> SMRObject.Builder<R> setTypeToken(TypeToken<R> typeToken) {
            this.type = ClassUtils.cast(typeToken.getRawType());
            return ClassUtils.cast(this);
        }

        public SMRObject.Builder<T> setArguments(Object ... arguments) {
            this.arguments = arguments;
            return this;
        }

        public SMRObject.Builder<T> setStreamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public SMRObject.Builder<T> setStreamID(UUID streamID) {
            this.streamID = streamID;
            return this;
        }

        public SMRObject.Builder<T> setSerializer(ISerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public SMRObject.Builder<T> addOpenOption(ObjectOpenOption openOption) {
            this.openOption = openOption;
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(Set<UUID> tags) {
            this.streamTags = new HashSet<>(tags);
            return this;
        }

        public SMRObject.Builder<T> setStreamTags(UUID... tags) {
            this.streamTags = new HashSet<>(Arrays.asList(tags));
            return this;
        }

        public SMRObject<T> build() {
            if (streamID == null && streamName != null) {
                streamID = UUID.nameUUIDFromBytes(streamName.getBytes());
            }
            verify();
            return new SMRObject<>(corfuRuntime, type, streamID, streamName,
                serializer, openOption, arguments, streamTags);
        }

        public T open() {
            final SMRObject<T> smrObject = build();

            String msg = "ObjectBuilder: open Corfu stream {} id {}";
            log.info(CorfuRuntime.LOG_NOT_IMPORTANT, msg, smrObject.getStreamName(), smrObject.getStreamID());

            var oid = new ObjectsView.ObjectID(streamID, type);

            final T result = getWrapper(smrObject);

            if (smrObject.getOpenOption() == ObjectOpenOption.NO_CACHE) {
                return result;
            }

            Function<ObjectID, T> tableFactory = x -> {
                // Get object serializer to check if we didn't attempt to set another serializer
                // to an already existing map
                ISerializer objectSerializer = result.getCorfuSMRProxy().getSerializer();
                if (smrObject.getSerializer() != objectSerializer) {
                    log.warn("open: Attempt to open an existing object with a different serializer {}. " +
                                    "Object {} opened with original serializer {}.",
                            smrObject.getSerializer().getClass().getSimpleName(),
                            oid,
                            objectSerializer.getClass().getSimpleName());
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
                log.error(errMsg, type, ex);
                throw new UnrecoverableCorfuError(ex);
            }
        }
    }

    public static <T extends ICorfuSMR<?>> Builder<T> builder() {
        return new Builder<>();
    }
}
