package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;

import java.util.EnumMap;

/**
 * Created by mwei on 2/1/16.
 */
public interface ILogUnitEntry extends IMetadata {

    /** Gets the type of result this entry represents.
     *
     * @return  The type of result this entry represents.
     */
    LogUnitReadResponseMsg.ReadResultType getResultType();

    /** Gets the metadata map.
     *
     * @return  A map containing the metadata for this entry.
     */
    EnumMap<LogUnitMetadataType, Object> getMetadataMap();

    /** Gets a ByteBuf representing the payload for this data.
     *
     * @return  A ByteBuf representing the payload for this data.
     */
    ByteBuf getBuffer();

    /** Gets the deserialized payload.
     *
     * @return  An object representing the deserialized payload.
     */
    Object getPayload(CorfuRuntime rt);

    /** Get an estimate of how large this entry is in memory.
     *
     * @return  An estimate on the size of this object, in bytes.
     */
    default int getSizeEstimate() {
        return 1; // The default is that we don't know, so we return 1.
    }
}
