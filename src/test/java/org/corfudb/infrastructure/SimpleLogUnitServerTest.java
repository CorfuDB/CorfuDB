package org.corfudb.infrastructure;

import static org.junit.Assert.assertEquals;

import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SimpleLogUnitServerTest {

    @Test
    public void checkIfLogUnitIsWritable() throws Exception
    {
        byte[] test = new byte[4096];
        for (int i = 0; i < 4096; i++)
        {
            test[i] = (byte)(i % 255);
        }
        SimpleLogUnitServer slus = new SimpleLogUnitServer();
        slus.reset();
        for (int i = 0; i < 100; i++)
        {
            ArrayList<Integer> epochlist = new ArrayList<Integer>();
            epochlist.add(0);
            ArrayList<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
            byteList.add(ByteBuffer.wrap(test));
            ErrorCode ec = slus.write(new UnitServerHdr(epochlist, i), byteList, ExtntMarkType.EX_FILLED);
            assertEquals(ec, ErrorCode.OK);
        }
    }
}
