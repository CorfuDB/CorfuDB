package org.corfudb.util;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by crossbach on 5/22/15.
 */

public class Utils
{
    public static ByteBuffer serialize(Object obj)
    {
        try
        {
            //todo: make serialization less clunky!
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte b[] = baos.toByteArray();
            oos.close();
            return ByteBuffer.wrap(b);
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    public static Object deserialize(ByteBuffer b)
    {
        try
        {
            //todo: make serialization less clunky!
            ByteArrayInputStream bais = new ByteArrayInputStream(b.array());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            return obj;
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
        catch(ClassNotFoundException ce)
        {
            throw new RuntimeException(ce);
        }
    }
}