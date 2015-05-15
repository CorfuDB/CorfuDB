package org.corfudb.runtime.protocols.logunits;

import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.UnwrittenException;
import org.junit.Test;

import java.util.HashMap;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mwei on 5/14/15.
 */
public class RedisLogUnitProtocolIT {
    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }

    @Test
    public void checkIfLogUnitIsPingable() throws Exception
    {
        RedisLogUnitProtocol mlup = new RedisLogUnitProtocol("localhost", 12900, null, 0L);
        assertTrue(mlup.ping());
    }

    @Test
    public void checkIfLogUnitIsWritable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        RedisLogUnitProtocol mlup = new RedisLogUnitProtocol("localhost", 12900, null, 0L);
        mlup.reset(0);
        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, test);
        }
    }

    @Test
    public void checkIfLogUnitIsWriteOnce() throws Exception
    {
        byte[] test = getTestPayload(4096);
        RedisLogUnitProtocol mlup = new RedisLogUnitProtocol("localhost", 12900, null, 0L);
        mlup.reset(0);
        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, test);
        }

        assertRaises(() -> mlup.write(42, test), OverwriteException.class);
    }

    @Test
    public void checkIfLogIsReadable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        RedisLogUnitProtocol mlup = new RedisLogUnitProtocol("localhost", 12900, null, 0L);
        mlup.reset(0);
        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, test);
        }

        byte[] data = mlup.read(42);
        assertArrayEquals(data, test);
    }

    @Test
    public void checkIfEmptyAddressesAreUnwritten() throws Exception
    {
        byte[] test = getTestPayload(4096);
        RedisLogUnitProtocol mlup = new RedisLogUnitProtocol("localhost", 12900, null, 0L);
        mlup.reset(0);
        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, test);
        }

        assertRaises(() -> mlup.read(101), UnwrittenException.class);
    }
}
