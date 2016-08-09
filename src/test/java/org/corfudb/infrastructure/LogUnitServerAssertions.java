package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.assertj.core.api.AbstractAssert;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.protocols.wireprotocol.LogUnitPayloadMsg;

import java.util.UUID;

/**
 * Created by mwei on 1/7/16.
 */
public class LogUnitServerAssertions extends AbstractAssert<LogUnitServerAssertions, LogUnitServer> {

    public LogUnitServerAssertions(LogUnitServer actual) {
        super(actual, LogUnitServerAssertions.class);
    }

    public static LogUnitServerAssertions assertThat(LogUnitServer actual) {
        return new LogUnitServerAssertions(actual);
    }


    public LogUnitServerAssertions isEmptyAtAddress(long address) {
        isNotNull();

        if (actual.dataCache.get(new LogAddress(address, null)) != null) {
            failWithMessage("Expected address <%d> to be empty but contained data!", address);
        }

        return this;
    }

    public LogUnitServerAssertions containsDataAtAddress(long address) {
        isNotNull();

        if (actual.dataCache.get(new LogAddress(address, null)) == null) {
            failWithMessage("Expected address <%d> to contain data but was empty!", address);
        } else if (actual.dataCache.get(new LogAddress(address, null)).isHole()) {
            failWithMessage("Expected address <%d> to contain data but was filled hole!", address);
        }

        return this;
    }

    public LogUnitServerAssertions containsFilledHoleAtAddress(long address) {
        isNotNull();

        if (actual.dataCache.get(new LogAddress(address, null)) == null) {
            failWithMessage("Expected address <%d> to contain filled hole but was empty!", address);
        } else if (!actual.dataCache.get(new LogAddress(address, null)).isHole()) {
            failWithMessage("Expected address <%d> to contain filled hole but was data!", address);
        }

        return this;
    }

    public LogUnitServerAssertions matchesDataAtAddress(long address, Object data) {
        isNotNull();

        if (actual.dataCache.get(new LogAddress(address, null)) == null) {
            failWithMessage("Expected address <%d> to contain data but was empty!", address);
        } else {
            actual.dataCache.get(new LogAddress(address, null)).getBuffer().resetReaderIndex();
            ByteBuf b = UnpooledByteBufAllocator.DEFAULT.buffer();
            LogUnitPayloadMsg.defaultSerializer.serialize(data, b);
            byte[] expected = new byte[b.readableBytes()];
            b.getBytes(0, expected);
            int actualbytes = actual.dataCache.get(new LogAddress(address, null)).getBuffer().readableBytes();
            byte[] actualb = new byte[actualbytes];
            actual.dataCache.get(new LogAddress(address, null)).getBuffer().getBytes(0, actualb);
            actual.dataCache.get(new LogAddress(address, null)).getBuffer().resetReaderIndex();

            org.assertj.core.api.Assertions.assertThat(actualb)
                    .isEqualTo(expected);
        }

        return this;
    }
}
