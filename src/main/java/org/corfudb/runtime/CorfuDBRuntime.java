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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.sharedlog.ExtntWrap;
import org.corfudb.sharedlog.UnwrittenCorfuException;


/**
 * @author mbalakrishnan
 *
 */
public class CorfuDBRuntime
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{

		if (args.length == 0)
		{
			System.out.println("usage: java CorfuDBRuntime masterURL");
			System.out.println("e.g. masterURL: http://localhost:8000/corfu");
			return;
		}

		String masternode = args[0];

		ClientLib crf;

		try
		{
			crf = new ClientLib(masternode);
		}
		catch (CorfuException e)
		{
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

		TXRuntime TR = new TXRuntime(new SMREngine(sb));
		//SimpleRuntime TR = new SimpleRuntime(sb);


		//counter test
		//CorfuDBObject cob = new CorfuDBCounter(TR, 1234);
		//map test
		CorfuDBMap<Integer, String> cob1 = new CorfuDBMap<Integer, String>(TR, 2345);
		CorfuDBMap<Integer, String> cob2 = new CorfuDBMap<Integer, String>(TR, 2346);


		numthreads = 2;
		for (int i = 0; i < numthreads; i++)
		{
			//linearizable tester
			//Thread T = new Thread(new CorfuDBTester(cob));
			//transactional tester
			Thread T = new Thread(new CorfuDBTXTester(cob1, cob2, TR));
			T.start();
		}
	}
}


interface AbstractRuntime
{
	void query_helper(CorfuDBObject cob);
	void update_helper(CorfuDBObject cob, Serializable update, Set<Long> streams);
	void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Set<Long> streams);
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
 * This class is a transactional runtime, implementing the AbstractRuntime interface
 * plus BeginTX/EndTX calls. It extends SimpleRuntime and overloads apply, update_helper and query_helper.
 */
class TXRuntime extends SimpleRuntime
{

	final ThreadLocal<TxInt> curtx = new ThreadLocal<TxInt>();

	//used to communicate decisions from the query_helper thread to waiting endtx calls
	final Map<Long, Boolean> decisionmap;

	public TXRuntime(SMREngine smre)
	{
		super(smre);
		decisionmap = new HashMap<Long, Boolean>();
	}

	void BeginTX()
	{
		if (curtx.get() != null) //there's already an executing tx
			throw new RuntimeException("tx already executing!"); //should we do something different to support nested txes?
		curtx.set(new TxInt());
	}


	boolean EndTX()
	{
		System.out.println("EndTX");
		long txpos = -1;
		//append the transaction intention
		//txpos = curbundle.append(BufferStack.serialize(curtx.get()), curtx.get().get_streams());
		//txpos = super.query_then_update_helper(null, null, curtx.get(), curtx.get().get_streams());
		txpos = smre.propose(curtx.get(), curtx.get().get_streams());
		//now that we appended the intention, we need to play the bundle until the append point
		smre.sync(txpos);
		//at this point there should be a decision
		//if not, for now we throw an error (but with decision records we'll keep syncing
		//until we find the decision)
		System.out.println("appended endtx at position " + txpos);
		synchronized (decisionmap)
		{
			if (decisionmap.containsKey(txpos))
			{
				boolean dec = decisionmap.get(txpos);
				decisionmap.remove(txpos);
				curtx.set(null);
				return dec;
			}
			else
				throw new RuntimeException("decision not found!");
		}
	}

	public void query_helper(CorfuDBObject cob)
	{
		if(curtx.get()==null) //non-transactional, pass through
		{
			super.query_helper(cob);
		}
		else
		{
			curtx.get().mark_read(cob.getID(), cob.getTimestamp());
		}
	}

	public void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Set<Long> streams)
	{
		if(curtx.get()==null) //not in a transactional context, append immediately to the streambundle as a singleton tx
		{
			//what about the weird case where the application proposes a TxInt? Can we assume
			//this won't happen since TxInt is not a public class?
			if(update instanceof TxInt) throw new RuntimeException("app cant update_helper a txint");
			//return super.query_then_update_helper(cob, query, update, streams);
			smre.propose(update, streams, query);
		}
		else //in a transactional context, buffer for now
		{
			if(query !=null)
			{
				//mark the read set
				query_helper(cob);
				//apply the precommand to the object
				apply(query, streams, SMREngine.TIMESTAMP_INVALID);
			}
			curtx.get().buffer_update(update, streams.iterator().next());
		}

	}

	public void update_helper(CorfuDBObject cob, Serializable update, Set<Long> streams)
	{
		query_then_update_helper(cob, null, update, streams);
	}

	public void apply(Object command, Set<Long> streams, long timestamp)
	{
		if (command instanceof TxInt)
		{
			TxInt T = (TxInt)command;
			if(process_txint(T, timestamp))
			{
				Iterator<Pair<Serializable, Long>> it = T.bufferedupdates.iterator();
				while(it.hasNext())
				{
					Pair<Serializable, Long> P = it.next();
					Set<Long> tstreams = new HashSet<Long>();
					tstreams.add(P.second);
					//todo: do we have to do 2-phase locking?
					//since all updates are funnelled through the apply thread
					//the only bad thing that can happen is that reads see a non-transactional state
					//but this can happen anyway, and in this case the transaction will abort
					super.apply(P.first, tstreams, timestamp);
				}
			}
		}
		else
			super.apply(command, streams, timestamp);
	}

	boolean process_txint(TxInt newtx, long timestamp)
	{
		synchronized(decisionmap)
		{
			System.out.println("decided position " + timestamp);
			decisionmap.put(timestamp, true);
			return true;
		}
	}
}

/**
 * This class performs state machine replication over a bundle of streams. It's unaware of transactions.
 * It can be directly used to implement SMR objects.
 *
 */
class SimpleRuntime implements AbstractRuntime, SMRLearner
{
	SMREngine smre;
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



	public SimpleRuntime(SMREngine tsmre)
	{
		smre = tsmre;
		smre.setLearner(this);
		objectmap = new HashMap<Long, CorfuDBObject>();

	}

	public void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Set<Long> streams)
	{
		smre.propose(update, streams, query);
	}
	
	public void update_helper(CorfuDBObject cob, Serializable update, Set<Long> streams)
	{
		query_then_update_helper(cob, null, update, streams);
	}


	/** returns once log has been played by playback thread
	 * until current tail.
	 */
	public void query_helper(CorfuDBObject cob)
	{

		smre.sync();
	}

	public void apply(Object command, Set<Long> streams, long timestamp)
	{
		if(streams.size()!=1) throw new RuntimeException("unimplemented");
		Long streamid = streams.iterator().next();
		synchronized(objectmap)
		{
			if(objectmap.containsKey(streamid))
			{
				CorfuDBObject cob = objectmap.get(streamid);
				cob.apply(command);
				//todo: verify that it's okay for this to not be atomic with the apply
				//in the worst case, the object thinks it has an older version that it really does
				//but all that should cause is spurious aborts
				//the alternative is to have the apply in the object always call a superclass version of apply
				//that sets the timestamp
				//only the apply thread sets the timestamp, so we only have to worry about concurrent reads
				if(timestamp!=SMREngine.TIMESTAMP_INVALID)
					cob.setTimestamp(timestamp);
			}
			else
				throw new RuntimeException("entry for stream " + streamid + " with no registered object");
		}

	}

}

interface SMRLearner
{
	void apply(Object command, Set<Long> streams, long timestamp);
}

/**
 * This class is an SMR engine. It's unaware of objects.
 */
class SMREngine
{
	SMRLearner smrlearner;

	//used to coordinate between querying threads and the query_helper thread
	Lock queuelock;
	List<Object> curqueue;

	StreamBundle curbundle;

	static final long TIMESTAMP_INVALID = -1;
	Lock pendinglock = new ReentrantLock();
	HashMap<Long, Pair<Serializable, Object>> pendingcommands = new HashMap<Long, Pair<Serializable, Object>>();

	public void setLearner(SMRLearner tlearner)
	{
		smrlearner = tlearner;
	}

	public SMREngine(StreamBundle sb)
	{
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


	public long propose(Serializable update, Set<Long> streams, Object precommand)
	{
		CommandWrapper cmd = new CommandWrapper(update, streams);
		pendinglock.lock();
		pendingcommands.put(cmd.uniqueid, new Pair(update, precommand));
		pendinglock.unlock();
		long pos = curbundle.append(BufferStack.serialize(cmd), streams);
		if(precommand!=null) //block until precommand is played
			sync(pos);
		return pos;
	}

	public long propose(Serializable update, Set<Long> streams)
	{
		return propose(update, streams, null);
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

	public void sync()
	{
		sync(-1);
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
		StreamEntry update = curbundle.readNext();
		while(update!=null)
		{
			CommandWrapper cmdw = (CommandWrapper)update.getPayload().deserialize();
			//if this command was generated by us, swap out the version we read back with the local version
			//this allows return values to be transmitted via the local command object
			pendinglock.lock();
			Pair<Serializable, Object> localcmds = null;
			if(pendingcommands.containsKey(cmdw.uniqueid))
				localcmds = pendingcommands.remove(cmdw.uniqueid);
			pendinglock.unlock();
			if(smrlearner==null) throw new RuntimeException("smr learner not set!");
			if(localcmds!=null)
			{
				if (localcmds.second != null)
				{
					smrlearner.apply(localcmds.second, cmdw.streams, TIMESTAMP_INVALID);
				}
				smrlearner.apply(localcmds.first, cmdw.streams, update.getLogpos());
			}
			else
				smrlearner.apply(cmdw.cmd, cmdw.streams, update.getLogpos());
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

class CorfuDBTXTester implements Runnable
{
	TXRuntime cr;
	CorfuDBMap map1;
	CorfuDBMap map2;
	public CorfuDBTXTester(CorfuDBMap tmap1, CorfuDBMap tmap2, TXRuntime tcr)
	{
		map1 = tmap1;
		map2 = tmap2;
		cr = tcr;
	}

	public void run()
	{
		System.out.println("starting thread");
		while(true)
		{
			int x = (int) (Math.random() * 1000.0);
			System.out.println("adding key " + x + " to both maps");
			cr.BeginTX();
			map1.put(x, "ABCD");
			map2.put(x, "XYZ");
			cr.EndTX();
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


abstract class CorfuDBObject
{
	//object ID -- corresponds to stream ID used underneath
	long oid;
	ReadWriteLock maplock;
	AbstractRuntime TR;

	AtomicLong timestamp;

	public long getTimestamp()
	{
		return timestamp.get();
	}

	public void setTimestamp(long newts)
	{
		timestamp.set(newts);
	}

	public void lock()
	{
		lock(false);
	}

	public void unlock()
	{
		unlock(false);
	}

	public void lock(boolean write)
	{
		if(write) maplock.writeLock().lock();
		else maplock.readLock().lock();
	}

	public void unlock(boolean write)
	{
		if(write) maplock.writeLock().unlock();
		else maplock.readLock().unlock();
	}

	abstract public void apply(Object update);

	public long getID()
	{
		return oid;
	}

	public CorfuDBObject(AbstractRuntime tTR, long tobjectid)
	{
		TR = tTR;
		oid = tobjectid;
		timestamp = new AtomicLong();
	}

}

class CorfuDBMap<K,V> extends CorfuDBObject implements Map<K,V>
{
	//backing state of the map
	Map<K, V> backingmap;

	public CorfuDBMap(AbstractRuntime tTR, long toid)
	{
		super(tTR, toid);
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
		else if(cc.getCmdType()==MapCommand.CMD_REMOVE)
		{
			backingmap.remove(cc.getKey());
		}
		else if(cc.getCmdType()==MapCommand.CMD_CLEAR)
		{
			backingmap.clear();
		}
		else
		{
			maplock.writeLock().unlock();
			throw new RuntimeException("Unrecognized command in stream!");
		}
		System.out.println("Map size is " + backingmap.size());
		maplock.writeLock().unlock();
	}

	@Override
	public int size()
	{
		TR.query_helper(this);
		//what if the value changes between query_helper and the actual read?
		//in the linearizable case, we are safe because we see a later version that strictly required
		//in the transactional case, the tx will spuriously abort, but safety will not be violated...
		lock();
		int x = backingmap.size();
		unlock();
		return x;
	}

	@Override
	public boolean isEmpty()
	{
		TR.query_helper(this);
		lock();
		boolean x = backingmap.isEmpty();
		unlock();
		return x;
	}

	@Override
	public boolean containsKey(Object o)
	{
		TR.query_helper(this);
		lock();
		boolean x = backingmap.containsKey(o);
		unlock();
		return x;
	}

	@Override
	public boolean containsValue(Object o)
	{
		TR.query_helper(this);
		lock();
		boolean x = backingmap.containsValue(o);
		unlock();
		return x;
	}

	@Override
	public V get(Object o)
	{
		TR.query_helper(this);
		lock();
		V x = backingmap.get(o);
		unlock();
		return x;
	}

	public V put(K key, V val)
	{
		HashSet<Long> H = new HashSet<Long>();
		H.add(this.getID());
		MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, key);
		TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_PUT, key, val), H);
		return (V)precmd.getReturnValue();
	}

	@Override
	public V remove(Object o)
	{
		//will throw a classcast exception if o is not of type K, which seems to expected behavior for the Map interface
		HashSet<Long> H = new HashSet<Long>();
		H.add(this.getID());
		MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, (K)o);
		TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_REMOVE, (K) o), H);
		return (V)precmd.getReturnValue();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map)
	{
		throw new RuntimeException("unimplemented");
	}

	@Override
	public void clear()
	{
		//will throw a classcast exception if o is not of type K, which seems to expected behavior for the Map interface
		HashSet<Long> H = new HashSet<Long>();
		H.add(this.getID());
		TR.update_helper(this, new MapCommand<K, V>(MapCommand.CMD_CLEAR), H);
	}

	@Override
	public Set<K> keySet()
	{
		throw new RuntimeException("unimplemented");
	}

	@Override
	public Collection<V> values()
	{
		throw new RuntimeException("unimplemented");

	}

	@Override
	public Set<Entry<K, V>> entrySet()
	{
		throw new RuntimeException("unimplemented");
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
	static final int CMD_REMOVE = 2;
	static final int CMD_CLEAR = 3;
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
	public MapCommand(int tcmdtype)
	{
		this(tcmdtype, null, null);
	}
	public MapCommand(int tcmdtype, K tkey)
	{
		this(tcmdtype, tkey, null);
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


class CorfuDBCounter extends CorfuDBObject
{
	//backing state of the counter
	int value;


	public CorfuDBCounter(AbstractRuntime tTR, long toid)
	{
		super(tTR, toid);
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
		lock(true);
		if(cc.getCmdType()==CounterCommand.CMD_DEC)
			value--;
		else if(cc.getCmdType()==CounterCommand.CMD_INC)
			value++;
		else
		{
			unlock(true);
			throw new RuntimeException("Unrecognized command in stream!");
		}
		unlock(true);
		System.out.println("Counter value is " + value);
	}
	public void increment()
	{
		HashSet<Long> H = new HashSet<Long>(); H.add(this.getID());
		TR.update_helper(this, new CounterCommand(CounterCommand.CMD_INC), H);
	}
	public int read()
	{
		TR.query_helper(this);
		//what if the value changes between queryhelper and the actual read?
		//in the linearizable case, we are safe because we see a later version that strictly required
		//in the transactional case, the tx will spuriously abort, but safety will not be violated...
		//todo: is there a more elegant API?
		lock(false);
		int ret = value;
		unlock(false);
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
	StreamEntry readNext();

	/**
	 * reads the next entry in the stream bundle that has a position strictly lower than stoppos.
	 * stoppos is required so that the runtime can check the current tail of the log using checkTail() and
	 * then play the log until that tail position and no further, in order to get linearizable
	 * semantics with a minimum number of reads.
	 *
	 * @param  stoppos  the stopping position for the read
	 * @return          the next entry in the stream bundle
	 */
	StreamEntry readNext(long stoppos);

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
		while(true)
		{
			try
			{
				ExtntWrap ew = cl.readExtnt(pos);
				//for now, copy to a byte array and return
				System.out.println("read back " + ew.getCtntSize() + " bytes");
				ret = new byte[4096 * 10]; //hack --- fix this
				ByteBuffer bb = ByteBuffer.wrap(ret);
				java.util.Iterator<ByteBuffer> it = ew.getCtntIterator();
				while (it.hasNext())
				{
					ByteBuffer btemp = it.next();
					bb.put(btemp);
				}
				break;
			}
			catch (UnwrittenCorfuException uce)
			{
				//encountered a hole -- try again
			}
			catch (CorfuException e)
			{
				throw new RuntimeException(e);
			}
		}
		return new BufferStack(ret);

	}
}

class StreamEntry
{
	private long logpos;
	private BufferStack payload;

	public long getLogpos()
	{
		return logpos;
	}

	public BufferStack getPayload()
	{
		return payload;
	}

	public StreamEntry(BufferStack tbs, long position)
	{
		logpos = position;
		payload = tbs;
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
	public StreamEntry readNext()
	{
		return readNext(0);
	}

	public synchronized StreamEntry readNext(long stoppos) //for now, using 'this' to synchronize curpos/curtail
	{
		if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
		{
			return null;
		}
		long readpos = curpos++;
		BufferStack ret = las.read(readpos);
		return new StreamEntry(ret, readpos);
	}
}