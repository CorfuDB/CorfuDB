package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.UUID;

/**
 * Created by mwei on 4/6/16.
 */
@Getter
@Builder
@AllArgsConstructor
public class SMRObject<T extends ICorfuSMR<T>> {

    CorfuRuntime runtime;

    Class<T> type;

    @SuppressWarnings("checkstyle:abbreviation")
    UUID streamID;

    String streamName;

    ISerializer serializer = Serializers.getDefaultSerializer();

    ObjectOpenOption option = ObjectOpenOption.CACHE;

    Object[] arguments = new Object[0];


    public static class SMRObjectBuilder<T extends ICorfuSMR<T>> {

        public Class<T> getType() {
            return this.type;
        }

        public CorfuRuntime getRuntime() {
            return this.runtime;
        }

        public UUID getStreamID() {
            return this.streamID;
        }

        private void verify() {
            if (streamName != null && !UUID.nameUUIDFromBytes(streamName.getBytes()).equals(streamID)) {
                throw new IllegalArgumentException("Stream id must be derived from stream name");
            }
        }

        @SuppressWarnings("unchecked")
        public <R extends ICorfuSMR<R>> SMRObject.SMRObjectBuilder<R>  setType(Class<R> type) {
            this.type = (Class<T>) type;
            return (SMRObject.SMRObjectBuilder<R>) this;
        }

        @SuppressWarnings("unchecked")
        public <R extends ICorfuSMR<R>> SMRObject.SMRObjectBuilder<R> setTypeToken(TypeToken<R> typeToken) {
            this.type = (Class<T>) typeToken.getRawType();
            return (SMRObject.SMRObjectBuilder<R>) this;
        }

        public SMRObject.SMRObjectBuilder<T> setArguments(Object ... arguments) {
            this.arguments = arguments;
            return this;
        }

        public SMRObject.SMRObjectBuilder<T> setStreamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public SMRObject.SMRObjectBuilder<T> setStreamID(UUID streamID) {
            this.streamID = streamID;
            return this;
        }

        public SMRObject.SMRObjectBuilder<T> setSerializer(ISerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public T build() {
            if (streamID == null && streamName != null) {
                streamID = UUID.nameUUIDFromBytes(streamName.getBytes());
            }
            verify();
            return (T) new SMRObject(runtime, type, streamID, streamName, serializer, option, arguments);
        }

        public T open() {
            return build();
        }
    }

}
