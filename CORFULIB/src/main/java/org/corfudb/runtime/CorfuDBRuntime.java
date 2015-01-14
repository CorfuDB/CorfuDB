package org.corfudb.runtime;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Stack;
import java.util.Iterator;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.sharedlog.ExtntWrap;

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

		if(args.length==0)
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

		List<Long> streams = new LinkedList<Long>();
		streams.add(new Long(1234)); //hardcoded hack
		CorfuDBRuntime TR = new CorfuDBRuntime(new StreamBundleImpl(streams, new CorfuStreamingSequencer(crf), new CorfuLogAddressSpace(crf)));

		CorfuDBCounter ctr = new CorfuDBCounter(TR, 1234);
		
		int numthreads = 1;
		for(int i=0;i<numthreads;i++)
		{
			Thread T = new Thread(new CorfuDBTester(ctr));
			T.start();
		}
	}

	Map<Long, CorfuDBObject> objectmap;
	StreamBundle curbundle;


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
			objectmap.put(obj.getID(), obj);
		}
	}

	public CorfuDBRuntime(StreamBundle sb)
	{
		objectmap = new HashMap<Long, CorfuDBObject>();
		curbundle = sb;
	}

	void queryhelper(long sid)
	{
		//later, asynchronously invoke the sync() thread
		//so that there's only one outstanding sync at a time
		sync();
	}

	void updatehelper(BufferStack update, long sid)
	{
		List<Long> streams = new LinkedList<Long>();
		streams.add(sid);
		curbundle.append(update, streams);
	}

	void sync()
	{
		long curtail = curbundle.checkTail();
		LogEntry update = curbundle.readNext(curtail);
		while(update!=null)
		{
			synchronized(objectmap)
			{
				if(objectmap.containsKey(update.streamid))
				{
					objectmap.get(update.streamid).upcall(update.payload);
				}
				else
					throw new RuntimeException("entry for stream with no registered object");
			}
			update = curbundle.readNext(curtail);
		}
	}
}

class BufferStack
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
		buffers.push(initialbuf);
	}
	public void push(byte[] buf)
	{
		buffers.push(buf);
		totalsize += buf.length;
	}
	public byte[] pop()
	{
		return buffers.pop();
	}
	public byte[] peek()
	{
		return buffers.peek();
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
}


/**
 * Tester for CorfuDBCounter
 *
 * @author mbalakrishnan
 *
 */

class CorfuDBTester implements Runnable
{
	CorfuDBCounter ctr;
	public CorfuDBTester(CorfuDBCounter tctr)
	{
		ctr = tctr;
	}

	public void run()
	{
		System.out.println("starting thread");
		while(true)
		{
			ctr.increment();
			System.out.println("counter value = " + ctr.read());
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


interface CorfuDBObject
{
	public void upcall(BufferStack update);
	public long getID();
}

class CorfuDBCounter implements CorfuDBObject
{
	//state of the counter
	int registervalue;
	
	CorfuDBRuntime TR;
	
	//object ID -- corresponds to stream ID used underneath
	long oid;
	
	//constants used in serialization
	static final int CMD_INC = 0;
	static final int CMD_DEC = 1;
	
	
	public long getID()
	{
		return oid;
	}
	
	public CorfuDBCounter(CorfuDBRuntime tTR, long toid)
	{
		registervalue = 0;
		TR = tTR;
		oid = toid;
		TR.registerObject(this);
	}
	public void upcall(BufferStack bs)
	{
		//System.out.println("dummyupcall");
		System.out.println("CorfuDBCounter received upcall");
		if(bs.numBufs()!=1)
			throw new RuntimeException("too few or too many bufs!");
		byte[] update = bs.pop();
		if(update[0]==CMD_INC) //increment
			registervalue++;
		else if(update[0]==CMD_DEC) //decrement
			registervalue--;
		else
			throw new RuntimeException("Unrecognized command in stream!");
		System.out.println("Setting value to " + registervalue);
	}
	public void increment()
	{
		//System.out.println("dummyinc");
		byte b[] = new byte[1];
		b[0] = CMD_INC;
		TR.updatehelper(new BufferStack(b), oid);
	}
	public void decrement()
	{
		//System.out.println("dummydec");
		byte b[] = new byte[1];
		b[0] = CMD_DEC;
		TR.updatehelper(new BufferStack(b), oid);
	}
	public int read()
	{
		TR.queryhelper(oid);
		return registervalue;
	}

}

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





class LogEntry
{
	public long streamid;
	BufferStack payload;
	public LogEntry(BufferStack tpayload, long tstreamid)
	{
		streamid = tstreamid;
		payload = tpayload;
	}
}




interface StreamBundle
{
	long append(BufferStack bs, List<Long> streams);

	/**
	 * reads the next entry in the stream bundle
	 *
	 * @return       the next log entry
	 */
	LogEntry readNext();

	/**
	 * reads the next entry in the stream bundle that has a position strictly lower than stoppos.
	 * stoppos is required so that the runtime can check the current tail of the log using checkTail() and
	 * then play the log until that tail position and no further, in order to get linearizable
	 * semantics with a minimum number of reads.
	 *
	 * @param  stoppos  the stopping position for the read
	 * @return          the next entry in the stream bundle
	 */
	LogEntry readNext(long stoppos);

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
	long get_slot(List<Long> streams);
	long check_tail();
}

class CorfuStreamingSequencer implements StreamingSequencer
{
	ClientLib cl;
	public CorfuStreamingSequencer(ClientLib tcl)
	{
		cl = tcl;
	}
	public long get_slot(List<Long> streams)
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
				throw new RuntimeException("multi-entry writes not yet implemented");
			LinkedList<ByteBuffer> buflist = new LinkedList<ByteBuffer>();
			//buflist.add(ByteBuffer.wrap(bs.flatten())); //this doesn't work since the logging layer wants extent-sized buffers
			byte[] payload = new byte[cl.grainsize()];
			ByteBuffer bb = ByteBuffer.wrap(payload);

			java.util.Iterator<byte[]> it = bs.iterator();
			while(it.hasNext())
				bb.put(it.next());
			buflist.add(bb);

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

	public long append(BufferStack bs, List<Long> streamids)
	{
		long ret = ss.get_slot(streamids);
		las.write(ret, bs);
		return ret;

	}

	public synchronized long checkTail() //for now, using 'this' to synchronize curtail
	{
		System.out.println("Checking tail...");
		curtail = ss.check_tail();
		System.out.println("tail is " + curtail);
		return curtail;

	}
	public LogEntry readNext()
	{
		return readNext(0);
	}

	public synchronized LogEntry readNext(long stoppos) //for now, using 'this' to synchronize curpos/curtail
	{
		if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
		{
			return null;
		}
		BufferStack ret = las.read(curpos++);
		return new LogEntry(ret, 1234); //hack --- faking streamid
	}
}