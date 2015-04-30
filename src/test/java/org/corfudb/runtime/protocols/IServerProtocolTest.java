package org.corfudb.runtime.protocols;

import org.junit.Before;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mwei on 4/30/15.
 */
public class IServerProtocolTest {

    Set<Class<? extends IServerProtocol>> protocols;

    @Before
    @SuppressWarnings("unchecked")
    public void getAllServerProtocols() {
        protocols = new HashSet<Class<? extends IServerProtocol>>();
        Reflections reflections = new Reflections("org.corfudb.runtime.protocols", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);

        for(Class<? extends Object> c : allClasses)
        {
            try {
                if (Arrays.asList(c.getInterfaces()).contains(IServerProtocol.class) && !c.isInterface())
                {
                    protocols.add((Class<? extends IServerProtocol>) c);
                }
            }
            catch (Exception e)
            {
            }
        }
    }

    @Test
    public void TestIfServerProtocolsImplementFactory() throws Exception{
        protocols.stream().forEach(p ->
        {
            assertTrue(p.getName() + " does not implement protocol factory!",
                    Arrays.stream(p.getMethods()).anyMatch(m->m.getName().equals("protocolFactory")));
        });
    }

    @Test
    public void TestIfServerContainsProtocolString() throws Exception {
        protocols.stream().forEach(p ->
        {
            assertTrue(p.getName() + " does not implement getProtocolString!",
                     Arrays.stream(p.getMethods()).anyMatch(m -> m.getName().equals("getProtocolString")));
            try {
                assertNotEquals(p.getName() + " does not override getProtocolString!",
                        p.getMethod("getProtocolString").invoke(null), "unknown");
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}
