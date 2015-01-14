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
package org.corfudb.sharedlog;

import org.corfudb.sharedlog.loggingunit.LogUnitConfigService;
import org.corfudb.sharedlog.loggingunit.LogUnitService;
import org.corfudb.sharedlog.loggingunit.LogUnitWrap;
import org.corfudb.sharedlog.sequencer.SequencerService;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

public class ClientLib implements
        ClientAPI,
        DbgAPI,
        ConfigAPI {

	Logger log = LoggerFactory.getLogger(ClientLib.class);

    static String master = "http://localhost:8000/corfu";
	static CorfuConfiguration CM;
    static public CorfuConfiguration pullConfig() throws CorfuException {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpGet httpget = new HttpGet(master);

            System.out.println("Executing request: " + httpget.getRequestLine());
            HttpResponse response = httpclient.execute(httpget);

            System.out.println("pullConfig from master: " + response.getStatusLine());

            CM = new CorfuConfiguration(response.getEntity().getContent());
        } catch (ClientProtocolException e) {
            throw new InternalCorfuException("cannot pull configuration");
        } catch (IOException e) {
            throw new InternalCorfuException("cannot pull configuration");
        }
        return CM;
    }


    SequencerService.Client sequencer;

    public ClientLib(String master) throws CorfuException {
        this.master = master;
		pullConfig();
		buildClientConnections();
	}
	
	void buildClientConnections() throws CorfuException {

		for (int g = 0; g < CM.getNumGroups(); g++) {
            for (Endpoint cn : CM.getGroupByNumber(g)) {
                if (cn == null) continue;
                try {
                    Endpoint.getSUnitOf(cn);
                } catch (CorfuException e) {
                    throw new CorfuException("buildConnections to sunits failed");
                }
            }
        }
		
		Endpoint sn = CM.getSequencer();
        try {
            sequencer = Endpoint.getSequencer(sn);
        } catch (CorfuException e) {
            throw new CorfuException("buildConnections to sequencer failed");
        }
    }

	/**
	 * Returns the size of a single Corfu entry
	 *
	 * @return		entry size
	 */
	@Override
	public int grainsize() throws CorfuException{
		return CM.getPagesize();
	}

	/**
	 * Breaks the bytebuffer is gets as parameter into grain-size buffers, and invokes appendExtnt(List<ByteBuffer>);
	 * 	see appendExtnt(List<ByteBuffer>, boolean) for more details
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	reqsize	size of buffer to append
	 * @return		see appendExtnt(List<ByteBuffer>, boolean)
	 * @throws 		@see appendExtnt(List<ByteBuffer>, boolean)
	 */
	@Override
	public long appendExtnt(byte[] buf, int reqsize) throws CorfuException {
		
		if (reqsize % grainsize() != 0) {
			throw new BadParamCorfuException("appendExtnt must be in multiples of log-entry size (" + grainsize() + ")");
		}

		int numents = reqsize/grainsize();
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>(numents);
		for (int i = 0; i < numents; i++)
			wbufs.add(ByteBuffer.wrap(buf, i*grainsize(), grainsize()));
		return appendExtnt(wbufs);
	}

	/**
	 * Appends an extent to the log. Extent will be written to consecutive log offsets.
	 *
	 * @param ctnt          list of ByteBuffers to be written
	 * @return              the first log-offset of the written range
	 * @throws CorfuException
	 * 		OutOfSpaceCorfuException indicates an attempt to append failed because storage unit(s) are full; user may try trim()
	 * 		OverwriteException indicates that an attempt to append failed because of an internal race; user may retry
	 * 		BadParamCorfuException or a general CorfuException indicate an internal problem, such as a server crash. Might not be recoverable
	 */
	@Override
	public long appendExtnt(List<ByteBuffer> ctnt) throws CorfuException {
		long offset = -1;
		
		try {
			offset = sequencer.nextpos(1); 
			writeExtnt(offset, ctnt);
		} catch (TException e) {
			e.printStackTrace();
            // TODO should we invoke a monitor handler here if monitor != null ??
			throw new CorfuException("append() failed");
		}
		return offset;
	}
	
	public void writeExtnt(long offset, List<ByteBuffer> ctnt) throws CorfuException {
		ErrorCode er = null;
		CorfuConfiguration.EntryLocation el = CM.getLocationForOffset(offset);
        Vector<Endpoint> replicas = el.group;

        LogUnitService.Client sunit;

        try {
            for (Endpoint ep : replicas) {
                if (ep == null) continue; // fault unit, removed from configuration
                sunit = Endpoint.getSUnitOf(ep);
                er = sunit.write(new UnitServerHdr(CM.getIncarnation(), offset), ctnt, ExtntMarkType.EX_FILLED);
                log.info("err={} write{} replica={} size={} marktype={}",
                        er,
                        offset,
                        ep,
                        ctnt.size(),
                        ExtntMarkType.EX_FILLED);
                if (!er.equals(ErrorCode.OK)) break;
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        if (er == null || er.equals(ErrorCode.ERR_STALEEPOCH)) {
            List<Integer> curepoch = new ArrayList<Integer>(CM.getIncarnation());
            pullConfig();
            if (Util.compareIncarnations(CM.getIncarnation(), curepoch) > 0) // obtained new configuration, worth a retry
                writeExtnt(offset, ctnt);
            else                            // TODO perhaps sleep and retry one more time??
                throw new ConfigCorfuException("append() failed: incarnation "
                        + curepoch
                        + " not responding, err="
                        + er);
        } else
        if (er.equals(ErrorCode.ERR_FULL)) {
            throw new OutOfSpaceCorfuException("append() failed: full");
        } else
        if (er.equals(ErrorCode.ERR_OVERWRITE)) {
            throw new OverwriteCorfuException("append() failed: overwritten");
        } else
        if (er.equals(ErrorCode.ERR_BADPARAM)) {
            throw new BadParamCorfuException("append() failed: bad parameter passed");
        } else
        if (er.equals(ErrorCode.ERR_STALEEPOCH)) {
            throw new BadParamCorfuException("append() failed: stale epoch");
        }
    }
	
	long lastReadOffset = -1;
		
	/**
	 * a variant of readExtnt that takes the first log-offset position to read the extent from.
	 * 
	 * @param offset           starting position to read
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	@Override
	public ExtntWrap readExtnt(long offset) throws CorfuException {
		
		ExtntWrap ret = null;
		CorfuConfiguration.EntryLocation el = CM.getLocationForOffset(offset);
        Endpoint ep = null;
        LogUnitService.Client sunit = null;

        for (ListIterator<Endpoint> it = el.group.listIterator(el.group.size());
             it.hasPrevious() && ep == null;
             ep = it.previous() )
            ;                                   // for-loop to find the tail of replica-chain to read from
        sunit = Endpoint.getSUnitOf(ep);

        try {
            log.debug("read offset {}", offset);
            ret = sunit.read(new UnitServerHdr(CM.getIncarnation(), offset));
        } catch (TException e) {
            e.printStackTrace();
        }

        if (ret == null || ret.getErr().equals(ErrorCode.ERR_STALEEPOCH) ) {
            List<Integer> curepoch = new ArrayList<Integer>(CM.getIncarnation());
            pullConfig();
            if (Util.compareIncarnations(CM.getIncarnation(), curepoch) > 0) // obtained new configuration, worth a retry
                return readExtnt(offset);
            else                        // TODO perhaps sleep and retry one more time??
                throw new ConfigCorfuException("read(" + offset + ") failed: configuration issue");
        } else if (ret.getErr().equals(ErrorCode.ERR_UNWRITTEN)) {
            throw new UnwrittenCorfuException("read(" + offset + ") failed: unwritten");
        } else if (ret.getErr().equals(ErrorCode.ERR_TRIMMED)) {
            lastReadOffset = CM.getTrimmark();
            throw new TrimmedCorfuException("read(" + offset + ") failed: trimmed");
        } else if (ret.getErr().equals(ErrorCode.ERR_BADPARAM)) {
            throw new OutOfSpaceCorfuException("read(" + offset + ") failed: bad parameter");
        }

        lastReadOffset = offset;
        return ret;
	}
		
	/**
	 * Reads the next extent; it remembers the last read extent (starting with zero).
	 * 
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	@Override
	public ExtntWrap readExtnt() throws CorfuException {
		
		ExtntWrap r;
		do {
			r = readExtnt(lastReadOffset+1);
		} while (r.getErr().equals(ErrorCode.OK_SKIP));
		return r;
	}

    /**
	 * force a delay until we are notified that previously invoked writes to the log have been safely forced to persistent store.
	 * 
	 * @throws CorfuException if the call to storage-units failed; in this case, there is no gaurantee regarding data persistence.
	 */
	@Override
	public void sync() throws CorfuException {

        for (Vector<Endpoint> grp : CM.getActiveSegmentView().getGroups()) {
            for (Endpoint ep : grp) {
                if (ep == null) continue; // fault unit, removed from configuration
				LogUnitService.Client sunit = Endpoint.getSUnitOf(ep);
				try { sunit.sync(); } catch (TException e) {
					throw new InternalCorfuException("sync() failed ");
				}
			}
		}
	}
	
	/**
	 * Query the log head. 
	 *  
	 * @return the current head's index 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	@Override
	public long queryhead() throws CorfuException { return CM.trimmark; }
	
	/**
	 * Query the log tail. 
	 *  
	 * @return the current tail's index 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	@Override
	public long querytail() throws CorfuException {
		long r;
		try {
			r = sequencer.nextpos(0);
		} catch (TException t) {
			throw new InternalCorfuException("querytail() failed ");
		}
		if (r < 0) throw new InternalCorfuException("querytail() call returned negative value, shouldn't happen");
		return r;
	}

	/**
	 * Query the last known checkpoint position. 
	 *  
	 * @return the last known checkpoint position.
	 * @throws CorfuException if the call fails or returns illegal (negative) value 
	 */
	@Override
	public long queryck() throws CorfuException {
		throw new BadParamCorfuException("queryck() not implemented yet");
	}
	
	/**
	 * inform about a new checkpoint mark. 
	 *  
	 * @param off the offset of the new checkpoint
	 * @throws CorfuException if the call fails 
	 */
	@Override
	public void ckpoint(long off) throws CorfuException {
		throw new BadParamCorfuException("ckpoint() not implemented yet");
	}
	
	/**
	 * set the read mark to the requested position. 
	 * after this, invoking readExtnt will perform at the specified position.
	 * 
	 * @param offset move the read mark to this log position
	 */
	@Override
	public void setMark(long offset) {
		lastReadOffset = offset-1;
	}
		
	// from here down, implement DbgAPI for debugging:
	// ==========================================================
	
	/**
	 * return the meta-info record associated with the specified offset. used for debugging.
	 * 
	 * @param offset the inquired position
	 * @return an ExtntInfo object
	 * @throws CorfuException
	 *     TrimmedCorfuException, BadParam, Unwritten, with the obvious meanings
	 */
	@Override
	public ExtntWrap dbg(long offset) throws CorfuException {
		CorfuConfiguration.EntryLocation el = CM.getLocationForOffset(offset);
		LogUnitService.Client sunit = Endpoint.getSUnitOf(el.group.elementAt(0));
		try {
			return sunit.readmeta(new UnitServerHdr(CM.getIncarnation(), offset));
		} catch (TException t) {
			throw new InternalCorfuException("dbg() failed ");
		}
	}
	
	/**
	 * utility function to grab tcnt tokens from the sequencer. used for debugging.
	 * 
	 * @param tcnt the number of tokens to grab from sequencer
	 * @throws CorfuException is thrown in case of unexpected communication problem with the sequencer
	 */
	@Override
	public void grabtokens(int tcnt) throws CorfuException {
		try {
			sequencer.nextpos(tcnt);
		} catch (TException t) {
			throw new InternalCorfuException("grabtoken failed");
		}
	}

	/* (non-Javadoc)
	 * @see DbgAPI#write(long, byte[])
	 */
	public void write(long offset, byte[] buf) throws CorfuException {
		if (buf.length % grainsize() != 0) {
			throw new BadParamCorfuException("appendExtnt must be in multiples of log-entry size (" + grainsize() + ")");
		}

		int numents = buf.length/grainsize();
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>(numents);
		for (int i = 0; i < numents; i++)
			wbufs.add(ByteBuffer.wrap(buf, i*grainsize(), grainsize()));
		writeExtnt(offset, wbufs);
	}

    // from here down, implement ConfigAPI interface for administration:
    // ==========================================================

    /**
     * trim a prefix of log up to the specified position
     *
     * @param offset the position to trim to (exclusive)
     *
     * @throws CorfuException
     */
    public void trim(long offset) throws CorfuException {
        proposeReconfig(CM.ConfTrim(offset));
    }

    public LogUnitWrap rebuild(long offset) throws CorfuException {
        CorfuConfiguration.EntryLocation el = CM.getLocationForOffset(offset);
        LogUnitConfigService.Client cnfg = Endpoint.getCfgOf(el.group.elementAt(0));
        LogUnitWrap ret = null;
        try {
            ret = cnfg.rebuild();
        } catch (TException t) {
            throw new InternalCorfuException("rebuild failed");
        }
        return ret;
    }

    public void proposeRemoveUnit(int gind, int rind) throws CorfuException {
        proposeReconfig(CM.ConfRemove(CM.getActiveSegmentView(), gind, rind) );
    }

    public void proposeDeployUnit(int gind, int rind, String hostname, int port) throws CorfuException {
        proposeReconfig(CM.ConfDeploy(CM.getActiveSegmentView(), gind, rind, hostname, port));
    }

    private void proposeReconfig(String proposal) throws CorfuException {

        DefaultHttpClient httpclient = new DefaultHttpClient();
        try {
            HttpPost httppost = new HttpPost(master);
            httppost.setEntity(new StringEntity(proposal));

            // System.out.println("Executing request: " + httppost.getRequestLine());
            HttpResponse response = httpclient.execute(httppost);

            log.info("proposal master status  -- {}", response.getStatusLine());
            BufferedReader b = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String resp = b.readLine();
            log.info("proposal master response -- {}", resp);
            if (resp.startsWith("approve")) {
                CM  = new CorfuConfiguration(proposal);
            }

        } catch (IOException e) {
            throw new InternalCorfuException("cannot propose  configuration");
        }
    }

    public void killUnit(int gind, int rind) throws CorfuException {
        Endpoint ep = CM.getEP(gind, rind);
        if (ep == null) throw new ConfigCorfuException("nothing to kill");

        try {
            Endpoint.getCfgOf(ep).kill();
        } catch (TException e) {
            e.printStackTrace();
            throw new ConfigCorfuException("cannot kill");
        }
    }

    /**
     * recover the token server by moving it to a known lower-bound on filled position
     * should only be used by administrative utilities
     *
     * @param lowbound
     */
    @Override
    public void tokenserverrecover(long lowbound) throws CorfuException {
        try {
            sequencer.recover(lowbound);
        } catch (TException e) {
            e.printStackTrace();
            throw new CorfuException("tokenserver recovery failed");
        }
    }

}
