package com.microsoft.corfu.unittests;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.ExtntInfo;

public class Junk implements Serializable {
	int i;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		ExtntInfo i = new ExtntInfo(1000000,0,0);
		byte[] bb = ExtntSerializer.serialize(i);
		System.out.println(i + " 's serialization sz: " + bb.length);
		
		ExtntInfo j = (ExtntInfo) ExtntSerializer.deserialize(bb);
		System.out.println("desrialize into: " + j);
	}

}

class ExtntSerializer {
    public static byte[] serialize(ExtntInfo obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeLong(obj.getMetaFirstOff());
  //      o.flush();
        System.out.println(b.size());
        o.writeInt(obj.getMetaLength());
 //       o.flush();
        System.out.println(b.size());
        o.writeInt(obj.getFlag());
        o.flush();
        return b.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        ExtntInfo i = new ExtntInfo();
        i.setMetaFirstOff(o.readLong());
        i.setMetaLength(o.readInt());
        i.setFlag(o.readInt());
        return i;
    }
}
