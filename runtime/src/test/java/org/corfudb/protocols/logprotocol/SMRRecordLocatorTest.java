package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by Xin Li on 05/23/19.
 */
public class SMRRecordLocatorTest {
    @Test
    public void testSMRRecordLocatorSerializeDeserialize() {

        long globalAddress = Address.getMinAddress();
        UUID steramId = UUID.randomUUID();
        int index = 0;
        int serializedSize = 0;

        SMRRecordLocator locator = new SMRRecordLocator(globalAddress, steramId, index, serializedSize);

        ByteBuf buf = Unpooled.buffer();
        locator.serialize(buf);
        SMRRecordLocator deserializedLocator = SMRRecordLocator.deserialize(buf);

        assertEquals(locator, deserializedLocator);
    }
}
