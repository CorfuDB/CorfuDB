package org.corfudb.runtime;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
			crf = new ClientLib(masternode); //hardcoded
		}
		catch (CorfuException e)
		{
			throw e;
		}

		//CorfuDBRuntime TR = new CorfuDBRuntime(new DummyStreamFactoryImpl());
		CorfuDBRuntime TR = new CorfuDBRuntime(new CorfuStreamFactoryImpl(crf));

		Thread T = new Thread(new CorfuDBTester(TR));
		T.start();
	}

	Map<Long, CorfuDBObject> objectmap;
	Map<Long, Stream> streammap;
	StreamFactory sf;

	public void registerObject(CorfuDBObject to)
	{
		//todo: check if the object already exists
		synchronized(objectmap)
		{
			if(objectmap.containsKey(to.getID()))
			{
				System.out.println("object ID already registered!");
				throw new RuntimeException();
			}
			objectmap.put(to.getID(), to);
			streammap.put(to.getID(), sf.new_stream(to.getID()));
		}
	}

	public CorfuDBRuntime(StreamFactory tsf)
	{
		sf = tsf;
		objectmap = new HashMap<Long, CorfuDBObject>();
		streammap = new HashMap<Long, Stream>();
	}

	void queryhelper(long sid)
	{
		//later, asynchronously invoke the sync() thread
		sync(sid);
	}

	void updatehelper(byte[] update, long sid)
	{
		Stream curstream = streammap.get(sid);
		//todo: stream doesn't exist
		curstream.append(new LogEntry(update, sid));
	}

	void sync(long sid)
	{
		Stream curstream = streammap.get(sid);
		//todo: stream doesn't exist
		long curtail = curstream.checkTail();
		LogEntry update = curstream.readNext(curtail);
		while(update!=null)
		{
//			System.out.println(update.streamid);
			synchronized(objectmap)
			{
				objectmap.get(update.streamid).upcall(update.payload); //todo: check for non-existence
			}
			update = curstream.readNext(curtail);
		}
	}

}

class CorfuDBTester implements Runnable
{

	CorfuDBRuntime TR;
	public CorfuDBTester(CorfuDBRuntime tTR)
	{
		TR = tTR;
	}

	public void run()
	{
		System.out.println("starting thread");
		CorfuDBCounter tr = new CorfuDBCounter(TR, 1234);
		while(true)
		{
			tr.increment();
			System.out.println("counter value = " + tr.read());
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
	int minbufsize = 128; //quickfix to handle min buffer size requirement of logging layer
	public void upcall(byte[] update);
	public long getID();
}

class CorfuDBCounter implements CorfuDBObject
{
	int registervalue;
	CorfuDBRuntime TR;
	long oid;
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
	public void upcall(byte update[])
	{
		//System.out.println("dummyupcall");
		System.out.println("CorfuDBCounter received upcall");
		if(update[0]==0) //increment
			registervalue++;
		else
			registervalue--;
		System.out.println("Setting value to " + registervalue);
	}
	public void increment()
	{
		//System.out.println("dummyinc");
		byte b[] = new byte[minbufsize]; //hardcoded
		b[0] = 0;
		TR.updatehelper(b, oid);
	}
	public void decrement()
	{
		//System.out.println("dummydec");
		byte b[] = new byte[minbufsize]; //hardcoded
		b[0] = 1;
		TR.updatehelper(b, oid);
	}
	public int read()
	{
		TR.queryhelper(oid);
		return registervalue;
	}

}

class CorfuDBRegister implements CorfuDBObject
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
	public void upcall(byte update[])
	{
//		System.out.println("dummyupcall");
		converter.put(update);
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
		TR.updatehelper(b, oid);
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






class LogEntry
{
	public long streamid;
	public byte[] payload;
	public LogEntry(byte[] tpayload, long tstreamid)
	{
		streamid = tstreamid;
		payload = tpayload;
	}
}

interface Stream
{
	long append(LogEntry le); //todo: should append be in the stream interface, since we'll eventually append to multiple streams?
	LogEntry readNext();
	LogEntry readNext(long stoppos);
	long checkTail();
}


interface StreamFactory
{
	public Stream new_stream(long streamid);
}

class DummyStreamFactoryImpl implements StreamFactory
{
	public Stream new_stream(long streamid)
	{
		return new DummyStreamImpl(streamid);
	}
}

class CorfuStreamFactoryImpl implements StreamFactory
{
	ClientLib cl;
	public CorfuStreamFactoryImpl(ClientLib tcl)
	{
		cl = tcl;
	}
	public Stream new_stream(long streamid)
	{
		return new CorfuStreamImpl(cl);
	}
}

class CorfuStreamImpl implements Stream
{
	long curpos;
	long curtail;
	ClientLib cl;
	public CorfuStreamImpl(ClientLib tcl)
	{
		cl = tcl;
		curpos = 0;
		curtail = 0;
	}

	@Override
	public long append(LogEntry le)
	{
		long ret;
		try
		{
			ret = cl.appendExtnt(le.payload, le.payload.length);
		}
		catch(CorfuException ce)
		{
			throw new RuntimeException(ce);
		}
		return ret;
	}

	@Override
	public LogEntry readNext()
	{
		return readNext(0);
	}

	@Override
	public synchronized LogEntry readNext(long stoppos) //for now, locking on this
	{
		if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
		{
			return null;
		}
		System.out.println("Reading..." + curpos);
		byte[] ret = null;
		try
		{
			ExtntWrap ew = cl.readExtnt();
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
		curpos++;
		return new LogEntry(ret, 1234); //hack --- faking streamid
	}

	@Override
	public synchronized long checkTail() //for now, locking on this
	{
		System.out.println("Checking tail...");
		try
		{
			curtail = cl.querytail();
		}
		catch (CorfuException e)
		{
			throw new RuntimeException(e);
		}
		System.out.println("tail is " + curtail);
		return curtail;
	}
}

class DummyStreamImpl implements Stream
{
	ArrayList<LogEntry> log;
	long curpos; //first unread entry
	long curtail; //total number of entries in log

	public synchronized long append(LogEntry entry)
	{
//		System.out.println("Dummy append");
		log.add(entry);
		return curtail++;
	}

	public synchronized LogEntry readNext()
	{
		return readNext(0);
	}

	public synchronized LogEntry readNext(long stoppos)
	{
//		System.out.println("Dummy read");
		if(curpos<curtail && (stoppos==0 || curpos<stoppos))
		{
			LogEntry ret = log.get((int)curpos);
			curpos++;
			return ret;
		}
		return null;
	}

	public synchronized long checkTail()
	{
		return curtail;
	}

	public DummyStreamImpl(long streamid)
	{
		log = new ArrayList<LogEntry>();
		curpos = 0;
		curtail = 0;
	}
}
