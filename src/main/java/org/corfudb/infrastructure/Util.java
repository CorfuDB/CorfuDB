/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.infrastructure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.UnitServerHdr;

public class Util {

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
        o.writeObject(obj.getFlag());
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
    public static ExtntInfo ExtntInfoDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        ExtntInfo i = new ExtntInfo();
        i.setMetaFirstOff(o.readLong());
        i.setMetaLength(o.readInt());
        i.setFlag((ExtntMarkType) o.readObject());
        return i;
    }

    /**
     * @return the size in bytes of an ExtntInfo object's serialization
     * @throws IOException
     */
    public static int ExtntInfoSSize() throws IOException {
    	return ByteBuffer.wrap(ExtntInfoSerialize(new ExtntInfo(0, 0, ExtntMarkType.EX_FILLED))).capacity();
    }

	public static byte[] ObjectSerialize(Object obj) throws IOException {
			ByteArrayOutputStream bb = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bb);
			oo.writeObject(obj);
			oo.flush();
			return bb.toByteArray();
	}

	public static Object ObjectDeserialize(byte[] buf) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bi = new ByteArrayInputStream(buf);
			ObjectInputStream oi = new ObjectInputStream(bi);
			return oi.readObject();
		}

    /**
     * Epoch management utils
     */

    /**
     * compare two incarnation values. Simply do a lexicographic comparison.
     *
     * @param e1
     * @param e2
     * @return the result of the comparison, encoded as follows:
     *          0: equal.
     *          -1: first parameter smaller than second.
     *          1: first param greater than second.
     */
    public static int compareIncarnations(List<Integer> e1, List<Integer> e2) {
        for (int j = 0; j < e1.size(); j++) {
            if (e1.get(j) < e2.get(j)) return -1;
            if (e1.get(j) > e2.get(j)) return 1;
        }
        return 0;
    }

    public static void incEpoch(List<Integer> e) {
        int epochind = e.size()-1;
        e.set(epochind, e.get(epochind)+1);
    }

    public static void incMasterEpoch(List<Integer> e, int masterid) {
        e.set(0, e.get(0)+1);
        e.set(1, masterid);
        e.set(2, 0);
    }

    public static void setIncarnation(List<Integer> e, int masterEpoch, int masterId, int epoch) {
        e.add(0, masterEpoch);
        e.add(1, masterId);
        e.add(2, epoch);
    }

    public static void setEpoch(List<Integer> e, int epoch) {
        int epochind = e.size()-1;
        e.set(epochind, epoch);
    }

    public static void setMasterEpoch(List<Integer> e, int masterEpoch, int masterid) {
        e.set(0, masterEpoch);
        e.set(1, masterid);
    }

    public static Integer getEpoch(List<Integer> e) {
        int epochind = e.size()-1;
        return e.get(epochind);
    }

    public static Integer getMasterEpoch(List<Integer> e) { return e.get(0); }
    public static Integer getMasterId(List<Integer> e) { return e.get(1); }
}
