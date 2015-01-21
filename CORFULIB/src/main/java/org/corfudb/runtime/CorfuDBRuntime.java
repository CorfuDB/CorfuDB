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

package org.corfudb.runtime;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.sharedlog.ExtntWrap;



/**
 * @author mbalakrishnan
 *
 */
public class CorfuDBRuntime extends SimpleRuntime {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length == 0) {
			System.out.println("usage: java CorfuDBRuntime masterURL");
			System.out.println("e.g. masterURL: http://localhost:8000/corfu");
			return;
		}

		String masternode = args[0];

		ClientLib crf;

		try {
			crf = new ClientLib(masternode);
		} catch (CorfuException e) {
			throw e;
		}


		int numthreads;


		List<Long> streams = new LinkedList<Long>();
		streams.add(new Long(1234)); //hardcoded hack
		streams.add(new Long(2345)); //hardcoded hack

		StreamBundle sb = new StreamBundleImpl(streams, new CorfuStreamingSequencer(crf), new CorfuLogAddressSpace(crf));

		//turn on to test stream bundle in isolation
/*
		numthreads = 2;
		for(int i=0;i<numthreads;i++)
		{
			Thread T = new Thread(new StreamBundleTester(sb));
			T.start();
		}
		Thread.sleep(10000);
		if(true) return;
*/

//		CorfuDBRuntime TR = new CorfuDBRuntime(sb);
		SimpleRuntime TR = new SimpleRuntime(sb);


		//counter test
		//CorfuDBObject cob = new CorfuDBCounter(TR, 1234);
		//map test
		CorfuDBObject cob = new CorfuDBMap<Integer, String>(TR, 2345);


		numthreads = 2;
		for (int i = 0; i < numthreads; i++) {
			Thread T = new Thread(new CorfuDBTester(cob));
			T.start();
		}
	}


	final ThreadLocal<TxInt> curtx = new ThreadLocal<TxInt>();

	//used to communicate decisions from the sync thread to waiting endtx calls
	final Map<Long, Boolean> decisionmap;


	public CorfuDBRuntime(StreamBundle sb) {
		super(sb);

		decisionmap = new HashMap<Long, Boolean>();


	}


/*	void apply(LogEntry update) {
		TxInt newtx = (TxInt) update.payload.deserialize();
		process_txint(newtx);
	}
*/

	void BeginTX() {
		if (curtx.get() != null) //there's already an executing tx
			throw new RuntimeException("tx already executing!"); //should we do something different to support nested txes?
		curtx.set(new TxInt());
	}


	boolean EndTX() {
		long txpos = -1;
		//append the transaction intention
		txpos = curbundle.append(BufferStack.serialize(curtx.get()), curtx.get().get_streams());
		//now that we appended the intention, we need to play the bundle until the append point
		sync(txpos);
		//at this point there should be a decision
		//if not, for now we throw an error (but with decision records we'll keep syncing
		//until we find the decision)
		synchronized (decisionmap) {
			if (decisionmap.containsKey(txpos)) {
				boolean dec = decisionmap.get(txpos);
				decisionmap.remove(txpos);
				return dec;
			} else
				throw new RuntimeException("decision not found!");
		}
	}


	public void sync()
	{
		if (curtx.get() == null) //not in a transactional context, sync immediately
		{
			super.sync();
		} else //transactional context, update read set of tx intention
		{
//			curtx.get().mark_read(cob.getID(), -1); //todo: what should the version number be?
		}
	}

	public void propose(Serializable update, Set<Long> streams, Object precommand)
	{
		throw new RuntimeException("unimplemented");
	}

	public void propose(Serializable update, Set<Long> streams)
	{
		if(streams.size()!=1) throw new RuntimeException("wrong number of streams!");

		if(curtx.get()==null) //not in a transactional context, append immediately to the streambundle
		{
			//create a singleton transaction
			TxInt tx = new TxInt();
			tx.buffer_update(update, streams.iterator().next());
			super.propose(tx, tx.get_streams());
		}
		else //in a transactional context, buffer for now
			curtx.get().buffer_update(update, streams.iterator().next());
	}

	
	void process_txint(TxInt newtx)
	{
		//is it a singleton write?
		if(newtx.get_readset().size()==0 && newtx.get_bufferedupdates().size()==1)
		{
			Pair<Serializable, Long> P = newtx.get_bufferedupdates().get(0);
			synchronized(objectmap)
			{
				if(objectmap.containsKey(P.second))
				{
					objectmap.get(P.second).apply(P.first);
				}
				else
					throw new RuntimeException("entry for stream with no registered object");
			}
		}
		else
			throw new RuntimeException("unimplemented");
	}
}


interface AbstractRuntime
{
	void sync();
	void sync(long stoppos);
	void propose(Serializable update, Set<Long> streams);
	void propose(Serializable update, Set<Long> streams, Object precommand);
	void registerObject(CorfuDBObject obj);
}

class CommandWrapper implements Serializable
{
	static long ctr=0;
	long uniqueid;
	Serializable cmd;
	Set<Long> streams;
	public CommandWrapper(Serializable tcmd, Set<Long> tstreams)
	{
		cmd = tcmd;
		//todo: for now, uniqueid is just a local counter; this won't work with multiple clients!
		uniqueid = ctr++;
		streams = tstreams;
	}
}

/**
 * This class performs state machine replication over a bundle of streams. It's unaware of transactions.
 * It can be directly used to implement SMR objects.
 *
 */
class SimpleRuntime implements AbstractRuntime
{
	//used to coordinate between querying threads and the sync thread
	Lock queuelock;
	List<Object> curqueue;
	
	StreamBundle curbundle;


	Map<Long, CorfuDBObject> objectmap;
	/**
	 * Registers an object with the runtime
	 *
	 * @param  obj  the object to register
	 */
	public void registerObject(CorfuDBObject obj)
	{
		synchronized(objectmap)
		{
			if(objectmap.containsKey(obj.getID()))
			{
				System.out.println("object ID already registered!");
				throw new RuntimeException();
			}
			System.out.println("registering object ID " + obj.getID());
			objectmap.put(obj.getID(), obj);
		}
	}



	public SimpleRuntime(StreamBundle sb)
	{
		objectmap = new HashMap<Long, CorfuDBObject>();

		curbundle = sb;
			
		queuelock = new ReentrantLock();
		curqueue = new LinkedList<Object>();

		//start the playback thread
		new Thread(new Runnable()
		{
			public void run()
			{
				while(true)
				{
					playback();
				}
			}
		}).start();

	}

	Lock pendinglock = new ReentrantLock();
	HashMap<Long, Pair<Serializable, Object>> pendingcommands = new HashMap<Long, Pair<Serializable, Object>>();

	public void propose(Serializable update, Set<Long> streams, Object precommand)
	{
		CommandWrapper cmd = new CommandWrapper(update, streams);
		pendinglock.lock();
		pendingcommands.put(cmd.uniqueid, new Pair(update, precommand));
		pendinglock.unlock();
		curbundle.append(BufferStack.serialize(cmd), streams);
	}
	
	public void propose(Serializable update, Set<Long> streams)
	{
		propose(update, streams, null);
	}


	/** returns once log has been played by playback thread
	 * until current tail.
	 */
	public void sync()
	{
		sync(-1);
	}

	/** returns once log has been played by playback thread
	* until syncpos.
	* todo: right now syncpos is ignored
	*/
	public void sync(long syncpos)
	{
		final Object syncobj = new Object();
		synchronized (syncobj)
		{
			queuelock.lock();
			curqueue.add(syncobj);
			queuelock.unlock();
			try
			{
				syncobj.wait();
			}
			catch (InterruptedException ie)
			{
				throw new RuntimeException(ie);
			}
		}
	}


	void apply(Object command, Set<Long> streams)
	{
		if(streams.size()!=1) throw new RuntimeException("unimplemented");
		Long streamid = streams.iterator().next();
		synchronized(objectmap)
		{
			if(objectmap.containsKey(streamid))
			{
				objectmap.get(streamid).apply(command);
			}
			else
				throw new RuntimeException("entry for stream " + streamid + " with no registered object");
		}

	}

	//runs in a single thread
	//todo: currently playback keeps running even if there are no queries; we need to run it on demand
	void playback()
	{
		queuelock.lock();
		//to ensure linearizability, any pending queries have to wait for the conclusion
		//a checkTail that started *after* they were issued. accordingly, when playback starts up,
		//it rotates out the current queue of pending requests to stop new requests from entering it
		List<Object> procqueue = curqueue;
		curqueue = new LinkedList<Object>();
		queuelock.unlock();


		//check the current tail of the bundle, and then read the bundle until that position
		long curtail = curbundle.checkTail();
		BufferStack update = curbundle.readNext();
		while(update!=null)
		{
			CommandWrapper cmdw = (CommandWrapper)update.deserialize();
			//if this command was generated by us, swap out the version we read back with the local version
			//this allows return values to be transmitted via the local command object
			pendinglock.lock();
			Pair<Serializable, Object> localcmds = null;
			if(pendingcommands.containsKey(cmdw.uniqueid))
				localcmds = pendingcommands.remove(cmdw.uniqueid);
			pendinglock.unlock();
			if(localcmds!=null)
			{
				if (localcmds.second != null)
				{
					apply(localcmds.second, cmdw.streams);
				}
				apply(localcmds.first, cmdw.streams);
			}
			else
				apply(cmdw.cmd, cmdw.streams);
			update = curbundle.readNext(curtail);
		}

		//wake up all waiting query threads; they will now see a state that incorporates all updates
		//that finished before they started
		Iterator it = procqueue.iterator();
		while(it.hasNext())
		{
			Object syncobj = it.next();
			synchronized(syncobj)
			{
				syncobj.notifyAll();
			}
		}
	}
}

//todo: custom serialization
class Pair<X, Y> implements Serializable
{
	final X first;
	final Y second;
	Pair(X f, Y s)
	{
		first = f;
		second = s;
	}

	public boolean equals(Pair<X,Y> otherP)
	{
		if(otherP==null) return false;
		if(((first==null && otherP.first==null) || first.equals(otherP.first)) //first matches up
		&& ((second==null && otherP.second==null) || (second.equals(otherP.second)))) //second matches up
			return true;
		return false;
	}
}

class TxInt implements Serializable //todo: custom serialization
{
	List<Pair<Serializable, Long>> bufferedupdates;
	Set<Long> streamset;
	Set<Pair<Long, Long>> readset;
	TxInt()
	{
		bufferedupdates = new LinkedList<Pair<Serializable, Long>>();
		readset = new HashSet<Pair<Long, Long>>();
		streamset = new HashSet<Long>();
	}
	void buffer_update(Serializable bs, long stream)
	{
		bufferedupdates.add(new Pair<Serializable, Long>(bs, stream));
		streamset.add(stream);
	}
	void mark_read(long object, long version)
	{
		readset.add(new Pair(object, version));
	}
	Set<Long> get_streams()
	{
		return streamset;
	}
	Set<Pair<Long, Long>> get_readset()
	{
		return readset;
	}
	List<Pair<Serializable, Long>> get_bufferedupdates()
	{
		return bufferedupdates;
	}
}


class BufferStack implements Serializable //todo: custom serialization
{
	private Stack<byte[]> buffers;
	private int totalsize;
	public BufferStack()
	{
		buffers = new Stack<byte[]>();
		totalsize = 0;
	}
	public BufferStack(byte[] initialbuf)
	{
		this();
		push(initialbuf);
	}
	public void push(byte[] buf)
	{
		buffers.push(buf);
		totalsize += buf.length;
	}
	public byte[] pop()
	{
		byte[] ret = buffers.pop();
		if(ret!=null)
			totalsize -= ret.length;
		return ret;
	}
	public byte[] peek()
	{
		return buffers.peek();
	}
	public int flatten(byte[] buf)
	{
		if(buffers.size()==0) return 0;
		if(buf.length<totalsize) throw new RuntimeException("buffer not big enough!");
		if(buffers.size()>1) throw new RuntimeException("unimplemented");
		System.arraycopy(buffers.peek(), 0, buf, 0, buffers.peek().length);
		return buffers.peek().length;
	}
	public byte[] flatten()
	{
		if(buffers.size()==1) return buffers.peek();
		else throw new RuntimeException("unimplemented");
	}
	public int numBufs()
	{
		return buffers.size();
	}
	public int numBytes()
	{
		return totalsize;
	}
	public java.util.Iterator<byte[]> iterator()
	{
		return buffers.iterator();
	}
	public static BufferStack serialize(Serializable obj)
	{
		try
		{
			//todo: custom serialization
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			byte b[] = baos.toByteArray();
			oos.close();
			return new BufferStack(b);
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
	public Object deserialize()
	{
		try
		{
			//todo: custom deserialization
			ByteArrayInputStream bais = new ByteArrayInputStream(this.flatten());
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


/**
 * Tester for CorfuDBCounter
 *
 * @author mbalakrishnan
 *
 */

class CorfuDBTester implements Runnable
{
	CorfuDBObject cob;
	public CorfuDBTester(CorfuDBObject tcob)
	{
		cob = tcob;
	}

	public void run()
	{
		System.out.println("starting thread");
		while(true)
		{
			if(cob instanceof CorfuDBCounter)
			{
				CorfuDBCounter ctr = (CorfuDBCounter)cob;
				ctr.increment();
				System.out.println("counter value = " + ctr.read());
			}
			else if(cob instanceof CorfuDBMap)
			{
				CorfuDBMap<Integer, String> cmap = (CorfuDBMap<Integer, String>)cob; //can't do instanceof on generics, have to guess
				int x = (int) (Math.random() * 1000.0);
				System.out.println("changing key " + x + " from " + cmap.put(x, "ABCD") + " to " + cmap.get(x));
			}
			try
			{
				Thread.sleep((int)(Math.random()*1000.0));
			}
			catch(Exception e)
			{
				throw new RuntimeException(e);
			}

		}
	}

}

class StreamBundleTester implements Runnable
{
	StreamBundle sb;
	public StreamBundleTester(StreamBundle tsb)
	{
		sb = tsb;
	}
	public void run()
	{
		System.out.println("starting sb tester thread");
		while(true)
		{
			int op = 0;
			if(op==0)
			{
				byte x[] = new byte[5];
				Set<Long> T = new HashSet<Long>();
				T.add(new Long(5));
				sb.append(new BufferStack(x), T);
			}
			else continue;
		}
	}
}


interface CorfuDBObject
{
	public void apply(Object update);
	public long getID();
}

class CorfuDBMap<K,V> implements CorfuDBObject
{
	//backing state of the map
	Map<K, V> backingmap;
	ReadWriteLock maplock;
	AbstractRuntime TR;
	//object ID -- corresponds to stream ID used underneath
	long oid;

	public long getID()
	{
		return oid;
	}

	public CorfuDBMap(AbstractRuntime tTR, long toid)
	{
		maplock = new ReentrantReadWriteLock();
		backingmap = new HashMap<K,V>();
		TR = tTR;
		oid = toid;
		TR.registerObject(this);
	}

	public void apply(Object bs)
	{
		//System.out.println("dummyupcall");
		System.out.println("CorfuDBMap received upcall");
		MapCommand<K,V> cc = (MapCommand<K,V>)bs;
		maplock.writeLock().lock();
		if(cc.getCmdType()==MapCommand.CMD_PUT)
		{
			backingmap.put(cc.getKey(), cc.getVal());
		}
		else if(cc.getCmdType()==MapCommand.CMD_PREPUT)
		{
			cc.setReturnValue(backingmap.get(cc.getKey()));
		}
		else
		{
			maplock.writeLock().unlock();
			throw new RuntimeException("Unrecognized command in stream!");
		}
		System.out.println("Map size is " + backingmap.size());
		maplock.writeLock().unlock();
	}
	public V put(K key, V val)
	{
		HashSet<Long> H = new HashSet<Long>();
		H.add(this.getID());
		MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, key);
		TR.propose(new MapCommand<K, V>(MapCommand.CMD_PUT, key, val), H, precmd);
		TR.sync();
		return (V)precmd.getReturnValue();
	}
	public V get(K key)
	{
		TR.sync();
		//what if the value changes between sync and the actual read?
		//in the linearizable case, we are safe because we see a later version that strictly required
		//in the transactional case, the tx will spuriously abort, but safety will not be violated...
		//todo: is there a more elegant API?
		maplock.readLock().lock();
		V val = backingmap.get(key);
		maplock.readLock().unlock();
		return val;
	}
}


abstract class SMRCommand implements Serializable
{

}

class MapCommand<K,V> extends SMRCommand
{
	int cmdtype;
	static final int CMD_PUT = 0;
	static final int CMD_PREPUT = 1;
	K key;
	V val;
	public K getKey()
	{
		return key;
	}
	public V getVal()
	{
		return val;
	}
	Object retval;
	public Object getReturnValue()
	{
		return retval;
	}
	public void setReturnValue(Object obj)
	{
		retval = obj;
	}
	public MapCommand(int tcmdtype, K tkey)
	{
		cmdtype = tcmdtype;
		key = tkey;
	}

	public MapCommand(int tcmdtype, K tkey, V tval)
	{
		cmdtype = tcmdtype;
		key = tkey;
		val = tval;
	}
	public int getCmdType()
	{
		return cmdtype;
	}
};


class CorfuDBCounter implements CorfuDBObject
{
	//backing state of the counter
	int value;

	ReadWriteLock valuelock;
	
	AbstractRuntime TR;
	
	//object ID -- corresponds to stream ID used underneath
	long oid;

	public long getID()
	{
		return oid;
	}
	
	public CorfuDBCounter(AbstractRuntime tTR, long toid)
	{
		valuelock = new ReentrantReadWriteLock();
		value = 0;
		TR = tTR;
		oid = toid;
		TR.registerObject(this);
	}
	public void apply(Object bs)
	{
		//System.out.println("dummyupcall");
		System.out.println("CorfuDBCounter received upcall");
		CounterCommand cc = (CounterCommand)bs;
		valuelock.writeLock().lock();
		if(cc.getCmdType()==CounterCommand.CMD_DEC)
			value--;
		else if(cc.getCmdType()==CounterCommand.CMD_INC)
			value++;
		else
		{
			valuelock.writeLock().unlock();
			throw new RuntimeException("Unrecognized command in stream!");
		}
		valuelock.writeLock().unlock();
		System.out.println("Counter value is " + value);
	}
	public void increment()
	{
		HashSet<Long> H = new HashSet<Long>(); H.add(this.getID());
		TR.propose(new CounterCommand(CounterCommand.CMD_INC), H);
	}
	public int read()
	{
		TR.sync();
		//what if the value changes between queryhelper and the actual read?
		//in the linearizable case, we are safe because we see a later version that strictly required
		//in the transactional case, the tx will spuriously abort, but safety will not be violated...
		//todo: is there a more elegant API?
		valuelock.readLock().lock();
		int ret = value;
		valuelock.readLock().unlock();
		return ret;
	}

}

class CounterCommand implements Serializable
{
	int cmdtype;
	static final int CMD_DEC = 0;
	static final int CMD_INC = 1;
	public CounterCommand(int tcmdtype)
	{
		cmdtype = tcmdtype;
	}
	public int getCmdType()
	{
		return cmdtype;
	}
};

/*class CorfuDBRegister implements CorfuDBObject
{
	ByteBuffer converter;
	int registervalue;
	CorfuDBRuntime TR;
	long oid;
	public long getID()
	{
		return oid;
	}

	public CorfuDBRegister(CorfuDBRuntime tTR, long toid)
	{
		registervalue = 0;
		TR = tTR;
		converter = ByteBuffer.wrap(new byte[minbufsize]); //hardcoded
		oid = toid;
		TR.registerObject(this);
	}
	public void upcall(BufferStack update)
	{
//		System.out.println("dummyupcall");
		converter.put(update.pop());
		converter.rewind();
		registervalue = converter.getInt();
		converter.rewind();
	}
	public void write(int newvalue)
	{
//		System.out.println("dummywrite");
		converter.putInt(newvalue);
		byte b[] = new byte[minbufsize]; //hardcoded
		converter.rewind();
		converter.get(b);
		converter.rewind();
		TR.updatehelper(new BufferStack(b), oid);
	}
	public int read()
	{
//		System.out.println("dummyread");
		TR.queryhelper(oid);
		return registervalue;
	}
	public int readStale()
	{
		return registervalue;
	}
}
*/







interface StreamBundle
{
	long append(BufferStack bs, Set<Long> streams);

	/**
	 * reads the next entry in the stream bundle
	 *
	 * @return       the next log entry
	 */
	BufferStack readNext();

	/**
	 * reads the next entry in the stream bundle that has a position strictly lower than stoppos.
	 * stoppos is required so that the runtime can check the current tail of the log using checkTail() and
	 * then play the log until that tail position and no further, in order to get linearizable
	 * semantics with a minimum number of reads.
	 *
	 * @param  stoppos  the stopping position for the read
	 * @return          the next entry in the stream bundle
	 */
	BufferStack readNext(long stoppos);

	/**
	 * returns the current tail position of the stream bundle (this is exclusive, so a checkTail
	 * on an empty stream returns 0). this also synchronizes local stream metadata with the underlying
	 * log and establishes a linearization point for subsequent readNexts; any subsequent readnext will
	 * reflect entries that were appended before the checkTail was issued.
	 *
	 * @return          the current tail of the stream
	 */
	long checkTail();
}


interface StreamingSequencer
{
	long get_slot(Set<Long> streams);
	long check_tail();
}

class CorfuStreamingSequencer implements StreamingSequencer
{
	ClientLib cl;
	public CorfuStreamingSequencer(ClientLib tcl)
	{
		cl = tcl;
	}
	public long get_slot(Set<Long> streams)
	{
		long ret;
		try
		{
			ret = cl.grabtokens(1);
		}
		catch(CorfuException ce)
		{
			throw new RuntimeException(ce);
		}
		return ret;
	}
	public long check_tail()
	{
		try
		{
			return cl.querytail();
		}
		catch(CorfuException ce)
		{
			throw new RuntimeException(ce);
		}
	}
}

interface LogAddressSpace
{
	void write(long pos, BufferStack bs);
	BufferStack read(long pos);
}

class CorfuLogAddressSpace implements LogAddressSpace
{
	ClientLib cl;

	public CorfuLogAddressSpace(ClientLib tcl)
	{
		cl = tcl;
	}

	public void write(long pos, BufferStack bs)
	{
		try
		{
			//convert to a linked list of extent-sized bytebuffers, which is what the logging layer wants
			if(bs.numBytes()>cl.grainsize())
				throw new RuntimeException("entry too big at " + bs.numBytes() + " bytes; multi-entry writes not yet implemented");
			LinkedList<ByteBuffer> buflist = new LinkedList<ByteBuffer>();
			byte[] payload = new byte[cl.grainsize()];
			bs.flatten(payload);
			buflist.add(ByteBuffer.wrap(payload));
			cl.writeExtnt(pos, buflist);
		}
		catch(CorfuException ce)
		{
			throw new RuntimeException(ce);
		}
	}

	public BufferStack read(long pos)
	{
		System.out.println("Reading..." + pos);
		byte[] ret = null;
		try
		{
			ExtntWrap ew = cl.readExtnt(pos);
			//for now, copy to a byte array and return
			System.out.println("read back " + ew.getCtntSize() + " bytes");
			ret = new byte[4096*10]; //hack --- fix this
			ByteBuffer bb = ByteBuffer.wrap(ret);
			java.util.Iterator<ByteBuffer> it = ew.getCtntIterator();
			while(it.hasNext())
			{
				ByteBuffer btemp = it.next();
				bb.put(btemp);
			}
		}
		catch (CorfuException e)
		{
			throw new RuntimeException(e);
		}
		return new BufferStack(ret);

	}
}

class StreamBundleImpl implements StreamBundle
{
	List<Long> mystreams;

	LogAddressSpace las;
	StreamingSequencer ss;

	long curpos;
	long curtail;


	public StreamBundleImpl(List<Long> streamids, StreamingSequencer tss, LogAddressSpace tlas)
	{
		las = tlas;
		ss = tss;
		mystreams = streamids;
	}

	//todo we are currently synchronizing on 'this' because ClientLib crashes on concurrent access;
	//once ClientLib is fixed, we need to clean up StreamBundle's locking
	public synchronized long append(BufferStack bs, Set<Long> streamids)
	{
		long ret = ss.get_slot(streamids);
		las.write(ret, bs);
		return ret;

	}

	public synchronized long checkTail() //for now, using 'this' to synchronize curtail
	{
//		System.out.println("Checking tail...");
		curtail = ss.check_tail();
//		System.out.println("tail is " + curtail);
		return curtail;

	}
	public BufferStack readNext()
	{
		return readNext(0);
	}

	public synchronized BufferStack readNext(long stoppos) //for now, using 'this' to synchronize curpos/curtail
	{
		if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
		{
			return null;
		}
		BufferStack ret = las.read(curpos++);
		return ret;
	}
}