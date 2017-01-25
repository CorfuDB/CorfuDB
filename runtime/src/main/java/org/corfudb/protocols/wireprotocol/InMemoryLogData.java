package org.corfudb.protocols.wireprotocol;

import org.corfudb.runtime.CorfuRuntime;

import java.util.EnumMap;

/** A class for in-memory log data, which is never serialized.
 *
 * Created by mwei on 1/5/17.
 */
public class InMemoryLogData implements ILogData {

    /** The type of data held by this LogData. */
    final DataType dataType;

    /** The object, unserialized, held by this LogData. */
    final Object data;

    /** The metadata map for this entry. */
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    /** Create a LogData without a payload.
     * @param dataType  The type this log data entry holds.
     */
    public InMemoryLogData(final DataType dataType) {
        this.dataType = dataType;
        this.data = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    /** Create a LogData with a payload.
     * @param dataType  The type this log data entry holds.
     * @param data      The underlying data.
     */
    public InMemoryLogData(final DataType dataType,
                           final Object data) {
        this.dataType = dataType;
        this.data = data;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    /** {@inheritDoc} */
    @Override
    public Object getPayload(CorfuRuntime t) {
        return data;
    }

    /** {@inheritDoc} */
    @Override
    public DataType getType() {
        return dataType;
    }

    /** {@inheritDoc} */
    @Override
    public EnumMap<LogUnitMetadataType, Object> getMetadataMap() {
        return metadataMap;
    }
}
