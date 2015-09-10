package org.corfudb.runtime.protocols.logunits;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;

import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.UnwrittenException;
import org.junit.Test;

import java.util.UUID;


/**
 * Created by mwei on 4/30/15.
 */
public class MemoryLogUnitProtocolTest {

    private static UUID uuid = UUID.randomUUID();

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
    public void checkIfLogUnitIsAlwaysPingable() throws Exception
    {
        MemoryLogUnitProtocol mlup = new MemoryLogUnitProtocol();
        assertTrue(mlup.ping());
    }

    @Test
    public void checkIfLogUnitIsWritable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        MemoryLogUnitProtocol mlup = new MemoryLogUnitProtocol();
        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, null, test);
        }
    }

    @Test
    public void checkIfLogUnitIsWriteOnce() throws Exception
    {
        byte[] test = getTestPayload(4096);
        MemoryLogUnitProtocol mlup = new MemoryLogUnitProtocol();

        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, null, test);
        }

        assertRaises(() -> mlup.write(42, null, test), OverwriteException.class);
    }

    @Test
    public void checkIfLogIsReadable() throws Exception
    {
        byte[] test = getTestPayload(4096);
        MemoryLogUnitProtocol mlup = new MemoryLogUnitProtocol();

        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, null, test);
        }

        byte[] data = mlup.read(42, uuid);
        assertArrayEquals(data, test);
    }

    @Test
    public void checkIfEmptyAddressesAreUnwritten() throws Exception
    {
        byte[] test = getTestPayload(4096);
        MemoryLogUnitProtocol mlup = new MemoryLogUnitProtocol();

        for (int i = 0; i < 100; i++)
        {
            mlup.write(i, null, test);
        }

        assertRaises(() -> mlup.read(101, uuid), UnwrittenException.class);
    }
}
