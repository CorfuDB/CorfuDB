package org.corfudb.client.gossip;

import java.io.Serializable;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.esotericsoftware.kryo.Kryo;
import java.util.Set;
import java.util.UUID;

public interface IGossip extends Serializable {
    static final long serialVersionUID = 0L;
    static void registerSerializer(Kryo kryo)
    {
        Reflections reflections = new Reflections("org.corfudb.client.gossip", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);

        for(Class<? extends Object> c : allClasses)
        {
            if (!c.equals(IGossip.class))
            {
                kryo.register(c);
            }
        }

        kryo.register(UUID.class);

    }
}
