package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.assertj.core.api.AbstractAssert;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;


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

        if (actual.getDataCache().get(address) != null) {
            failWithMessage("Expected address <%d> to be empty but contained data!", address);
        }

        return this;
    }

    public LogUnitServerAssertions containsDataAtAddress(long address) {
        isNotNull();

        if (actual.getDataCache().get(address) == null) {
            failWithMessage("Expected address <%d> to contain data but was empty!", address);
        } else if (actual.getDataCache().get(address).isHole()) {
            failWithMessage("Expected address <%d> to contain data but was filled hole!", address);
        }

        return this;
    }

    public LogUnitServerAssertions containsFilledHoleAtAddress(long address) {
        isNotNull();

        if (actual.getDataCache().get(address) == null) {
            failWithMessage("Expected address <%d> to contain filled hole but was empty!", address);
        } else if (!actual.getDataCache().get(address).isHole()) {
            failWithMessage("Expected address <%d> to contain filled hole but was data!", address);
        }

        return this;
    }

    public LogUnitServerAssertions matchesDataAtAddress(long address, Object data) {
        isNotNull();

        if (actual.getDataCache().get(address) == null) {
            failWithMessage("Expected address <%d> to contain data but was empty!", address);
        } else {
            ByteBuf b = Unpooled.buffer();
            Serializers.CORFU.serialize(data, b);
            byte[] expected = new byte[b.readableBytes()];
            b.getBytes(0, expected);

            org.assertj.core.api.Assertions.assertThat(((LogData)actual.getDataCache()
                    .get(address)).getData())
                    .isEqualTo(expected);

        }

        return this;
    }

    public LogUnitServerAssertions hasCorrectCacheSize(double ratio) {
        long maxHeapSize = Runtime.getRuntime().maxMemory();
        if(actual.getMaxCacheSize() != (long) (maxHeapSize * ratio)) {
            failWithMessage("Expected cache size <%d> doesn't match allocated cache size <%d>",
                    (long) (maxHeapSize * ratio), actual.getMaxCacheSize());
        }

        return this;
    }
}
