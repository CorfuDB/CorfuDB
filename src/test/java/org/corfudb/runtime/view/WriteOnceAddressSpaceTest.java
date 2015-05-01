package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.UnwrittenException;
import org.corfudb.runtime.smr.Pair;
import org.junit.Before;
import org.junit.Test;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by mwei on 4/30/15.
 */
public class WriteOnceAddressSpaceTest {

    private CorfuDBRuntime cdr;

    @Before
    public void getRuntime()
    {
        cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
    }

    @Test
    public void AddressSpaceIsWritable() throws Exception
    {
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        String testString = "hello world";
        woas.write(0, testString);
    }

    @Test
    public void AddressSpaceIsWriteOnce() throws Exception
    {
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        String testString = "hello world";
        woas.write(0, testString);
        assertRaises(() -> woas.write(0, testString), OverwriteException.class);
    }

    @Test
    public void AddressSpaceIsReadable() throws Exception
    {
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        String testString = "hello world";
        woas.write(0, testString);
        assertEquals(woas.readObject(0), testString);
    }

    @Test
    public void AddressSpaceReturnsUnwritten() throws Exception
    {
        WriteOnceAddressSpace woas = new WriteOnceAddressSpace(cdr);
        String testString = "hello world";
        woas.write(0, testString);
        assertRaises(() -> woas.read(1), UnwrittenException.class);
    }
}
