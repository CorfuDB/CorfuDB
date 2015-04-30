package org.corfudb.runtime.gossip;

import java.io.Serializable;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.esotericsoftware.kryo.Kryo;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public interface IGossip extends Serializable {
    static final long serialVersionUID = 0L;

    static void registerSerializer(Kryo kryo)
    {
        Reflections reflections = new Reflections("org.corfudb.client.gossip", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        ArrayList<Class<? extends Object>> sortedClasses = new ArrayList<Class<? extends Object>>(allClasses);
        Collections.sort(sortedClasses, new Comparator<Class<? extends Object>>() {
            public int compare (Class<? extends Object> c1, Class<? extends Object> c2)
            {
                return c1.getName().compareTo(c2.getName());
            }
        });

        for(Class<? extends Object> c : sortedClasses)
        {
            if (!c.equals(IGossip.class))
            {
                kryo.register(c);
            }
        }

        kryo.register(UUID.class);
        kryo.register(byte[].class);
        kryo.register(HashMap.class);

    }
}
