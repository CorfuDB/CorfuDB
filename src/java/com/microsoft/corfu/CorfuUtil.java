package com.microsoft.corfu;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class CorfuUtil {

	/** utility method to copy one Extnt meta-info record to another
	 * @param from source ExtntInfo
	 * @param to target ExtntInfo
	 */
	public static void ExtntInfoCopy(ExtntInfo from, ExtntInfo to) {
		to.setFlag(from.getFlag());
		to.setMetaFirstOff(from.getMetaFirstOff());
		to.setMetaLength(from.getMetaLength());
	}
	
	/** utility method to compute the log-offset succeeding an extent
	 * @param inf the extent's meta-info
	 * @return the offset succeeding this extent
	 */
	public static long ExtntSuccessor(ExtntInfo inf) { return inf.getMetaFirstOff() + inf.getMetaLength(); }

	/**
	 * 	utility method to serialize ExtntInfo objects
	 * 
	 * @param obj to serialize
	 * @return a byte array containing the object serialization
	 * @throws IOException shouldn't happen, since we are not doing actual IO, we are serializing into a memory buffer
	 */
    public static byte[] ExtntInfoSerialize(ExtntInfo obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeLong(obj.getMetaFirstOff());
        o.writeInt(obj.getMetaLength());
        o.writeInt(obj.getFlag());
        o.flush();
        return b.toByteArray();
    }

    /**
     * utility method to de-serialize a byte-array into a ExtntInfo object
     * 
     * @param bytes the byte array containing the object's serialization
     * @return the reconstructed object
     * @throws IOException shouldn't happen, since we are not doing actual IO, we are de-serializing from a memory buf
     * @throws ClassNotFoundException
     */
    public static ExtntInfo ExtntInfoDeserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        ExtntInfo i = new ExtntInfo();
        i.setMetaFirstOff(o.readLong());
        i.setMetaLength(o.readInt());
        i.setFlag(o.readInt());
        return i;
    }

    /**
     * @return the size in bytes of an ExtntInfo object's serialization
     * @throws IOException 
     */
    public static int ExtntInfoSSize() throws IOException { 
    	return ByteBuffer.wrap(ExtntInfoSerialize(new ExtntInfo(0, 0, 0))).capacity();
    }
}
