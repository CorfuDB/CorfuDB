package org.corfudb.runtime.view;

import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.UnwrittenException;
import org.junit.Before;
import org.junit.Test;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by mwei on 6/3/15.
 */
public abstract class IWriteOnceAddressSpaceTest {

    IWriteOnceAddressSpace addressSpace;

    protected abstract IWriteOnceAddressSpace getAddressSpace();

    @Before
    public void setupAddressSpace()
    {
        addressSpace = getAddressSpace();
    }

    @Test
    public void AddressSpaceIsWritable() throws Exception
    {
        String testString = "hello world";
        addressSpace.write(0, testString);
    }

    @Test
    public void AddressSpaceIsWriteOnce() throws Exception
    {
        String testString = "hello world";
        String testString2 = "hello world2";
        addressSpace.write(0, testString);
        assertRaises(() -> addressSpace.write(0, testString2), OverwriteException.class);
    }

    @Test
    public void AddressSpaceIsReadable() throws Exception
    {
        String testString = "hello world";
        addressSpace.write(0, testString);
        assertEquals(addressSpace.readObject(0), testString);
    }

    @Test
    public void AddressSpaceReturnsUnwritten() throws Exception
    {
        String testString = "hello world";
        addressSpace.write(0, testString);
        assertRaises(() -> addressSpace.read(1), UnwrittenException.class);
    }
}
