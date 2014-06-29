package com.microsoft.corfu;

import com.microsoft.corfu.loggingunit.LogUnitConfigService;
import com.microsoft.corfu.loggingunit.LogUnitService;
import com.microsoft.corfu.loggingunit.LogUnitWrap;
import com.microsoft.corfu.sequencer.SequencerService;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

    String master;
	CorfuConfiguration CM;
	SequencerService.Client sequencer;
	TTransport[] transports;

    public ClientLib(CorfuConfiguration CM) throws CorfuException {
        log.warn("CurfuClientImpl logging level = dbg?{} info?{} warn?{} err?{}",
                log.isDebugEnabled(), log.isInfoEnabled(), log.isWarnEnabled(), log.isErrorEnabled());
        this.CM = CM;
        buildClientConnections();
    }

	public ClientLib(String master) throws CorfuException {
		
		log.warn("CurfuClientImpl logging level = dbg?{} info?{} warn?{} err?{}", 
				log.isDebugEnabled(), log.isInfoEnabled(), log.isWarnEnabled(), log.isErrorEnabled());
        this.master = master;
		CM = pullConfig();
		buildClientConnections();
	}
	
	class clientSunitEndpoint {
		TTransport t = null;
		LogUnitService.Client cl = null;
		TBinaryProtocol protocol = null;
        LogUnitConfigService.Client configcl = null;

        clientSunitEndpoint(Endpoint cn) throws CorfuException {
            TMultiplexedProtocol mprotocol = null, mprotocol2 = null;

			try {
				t = new TSocket(cn.getHostname(), cn.getPort());
                t.open();
				protocol = new TBinaryProtocol(t);

                mprotocol = new TMultiplexedProtocol(protocol, "SUNIT");
				cl = new LogUnitService.Client(mprotocol);
				log.info("client connection open with multiplexed server  {}:{}" , cn.getHostname() , cn.getPort());

                mprotocol2 = new TMultiplexedProtocol(protocol, "CONFIG");
                configcl = new LogUnitConfigService.Client(mprotocol2);
                log.info("config  connection open with multiplexed server  {}:{}" , cn.getHostname() , cn.getPort());

			} catch (TTransportException e) {
				e.printStackTrace();
				throw new CorfuException("could not set up connection(s)");
			}		
		}
	}

    LogUnitService.Client getSUnitOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.cl;
    }
    LogUnitConfigService.Client getCfgOf(Endpoint cn) throws CorfuException {
        clientSunitEndpoint ep = (clientSunitEndpoint) cn.getInfo();
        if (ep == null) {
            ep = new clientSunitEndpoint(cn);
            cn.setInfo(ep);
        }
        return ep.configcl;
    }

	class clientSequencerEndpoint {
		TTransport t = null;
		SequencerService.Client cl = null;
		TBinaryProtocol protocol = null;
		
		clientSequencerEndpoint(Endpoint cn) throws CorfuException {
			try {
				t = new TSocket(cn.getHostname(), cn.getPort());
				protocol = new TBinaryProtocol(t);
				cl = new SequencerService.Client(protocol);
				t.open();
				log.info("client connection open with sequencer {}:{}", cn.getHostname(), cn.getPort());
			} catch (TTransportException e) {
				e.printStackTrace();
				throw new CorfuException("could not set up connection(s)");
			}		
		}
	}
	
	void buildClientConnections() throws CorfuException {

		for (int g = 0; g < CM.getNumGroups(); g++)
            for (Endpoint cn : CM.getGroupByNumber(g)) getSUnitOf(cn);
		
		Endpoint sn = CM.getSequencer();
		clientSequencerEndpoint seq = new clientSequencerEndpoint(sn);
        sn.setInfo(seq);
		sequencer = seq.cl;
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
			throw new CorfuException("append() failed");
		}
		return offset;
	}
	
	public void writeExtnt(long offset, List<ByteBuffer> ctnt) throws CorfuException {
		ErrorCode er = null;
		EntryLocation el = CM.getLocationForOffset(offset);
        Vector<Endpoint> replicas = el.group;
        int retrycnt = 0;

        LogUnitService.Client sunit;

        while (retrycnt < 5) {
            try {
                log.debug("write({} size={} marktype={}", offset, ctnt.size(), ExtntMarkType.EX_FILLED);
                for (Endpoint ep : replicas) {
                    if (ep == null) continue; // fault unit, removed from configuration
                    sunit = getSUnitOf(ep);
                    er = sunit.write(new UnitServerHdr(CM.getEpoch(), offset), ctnt, ExtntMarkType.EX_FILLED);
                }
            } catch (TException e) {
                e.printStackTrace();
                throw new CorfuException("append() failed");
            }

            if (er.equals(ErrorCode.ERR_STALEEPOCH)) {
                log.info("appendExtnt fails, stale epoch {}; pulling new epoch from Config manager", CM.getEpoch());
                pullConfig();
                retrycnt++; continue; //retry
            }

            if (er.equals(ErrorCode.ERR_FULL)) {
                throw new OutOfSpaceCorfuException("append() failed: full");
            } else if (er.equals(ErrorCode.ERR_OVERWRITE)) {
                throw new OverwriteCorfuException("append() failed: overwritten");
            } else if (er.equals(ErrorCode.ERR_BADPARAM)) {
                throw new BadParamCorfuException("append() failed: bad parameter passed");
            }

            return;
        }
        throw new CorfuException("append() failed: too many retries");
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
		
		ExtntWrap ret;
		EntryLocation el = CM.getLocationForOffset(offset);
        Endpoint ep;
        LogUnitService.Client sunit = null;
        int retrycnt=0;

        while(retrycnt < 5) {

            ListIterator<Endpoint> it = el.group.listIterator(el.group.size());
            while (it.hasPrevious()) {
                ep = it.previous();
                if (ep == null) continue;
                sunit = getSUnitOf(ep); // read from tail of replica-chain
                break;
            }

            try {
                log.debug("read offset {} now", offset);
                ret = sunit.read(new UnitServerHdr(CM.getEpoch(), offset));
            } catch (TException e) {
                e.printStackTrace();
                throw new CorfuException("read() failed");
            }

            if (ret.getErr().equals(ErrorCode.ERR_STALEEPOCH)) {
                log.info("readExtnt({}) fails, epoch changed", offset);
                pullConfig();
                retrycnt++; continue; // retry
            } else if (ret.getErr().equals(ErrorCode.ERR_UNWRITTEN)) {
                log.info("readExtnt({}) fails, unwritten", offset);
                throw new UnwrittenCorfuException("read(" + offset + ") failed: unwritten");
            } else if (ret.getErr().equals(ErrorCode.ERR_TRIMMED)) {
                lastReadOffset = offset;
                log.info("readExtnt({}) fails, trimmed", offset);
                throw new TrimmedCorfuException("read(" + offset + ") failed: trimmed");
            } else if (ret.getErr().equals(ErrorCode.ERR_BADPARAM)) {
                log.info("readExtnt({}) fails, bad param", offset);
                throw new OutOfSpaceCorfuException("read(" + offset + ") failed: bad parameter");
            }

            log.debug("read succeeds {}", offset);
            lastReadOffset = offset;
            return ret;
        }
        throw new CorfuException("read(" + offset + ") failed: too many retries");
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

	
	@Override
	public long repairNext() throws CorfuException {
		long head, tail;
		long offset = -1;
/* 		ErrorCode readErr = ErrorCode.OK;
		boolean skip = false;
		
		// check current log bounds
		head = queryhead(); 
		tail = querytail(); 
		
		offset = lastReadOffset+1;
		
		if (offset < head) {
			log.info("repairNext repositioning to head={}", head);
			offset = head;
		}
		if (offset >= tail) {
			log.info("repairNext reached log tail, finishing");
			return offset; // TODO do something??
		}
				
		// next, try to read 'offset' to see what is the error value
		EntryLocation el = CM.getLocationForOffset(offset);
		LogUnitService.Client sunit = getSUnitOf(el.group.replicas[0]);
		try {
			log.debug("repairNext read({})", offset);
			ExtntWrap ret = sunit.read(genHeader(offset));
			readErr = ret.getErr();
			if (!readErr.equals(ErrorCode.OK)) skip = true;
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("repairNext read() failed, communication problem; quitting");
		}
		
		// finally, if either the meta-info was broken, or the content was broken, skip will be ==true.
		// in that case, we mark this entry for skipping, and we also try to mark it for skip on the storage units
		
		if (!skip) {
			lastReadOffset = offset;
			return offset;
		}
		
		lastReadOffset = offset;
			
		// now we try to fix 'inf'
		ErrorCode er;
		log.debug("repairNext fix {}", offset);
		try {
			er = sunit.fix(genHeader(offset));
		} catch (TException e1) {
			e1.printStackTrace();
			throw new CorfuException("repairNext() fix failed, communication problem; quitting");
		}
		if (er.equals(ErrorCode.ERR_FULL)) {
			// TODO should we try to trim the log to the latest checkpoint and/or check if the extent range exceeds the log capacity??
			throw new OutOfSpaceCorfuException("repairNext failed, log full");
		}
*/
        return offset;
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
				LogUnitService.Client sunit = getSUnitOf(ep);
				try { sunit.sync(); } catch (TException e) {
					throw new InternalCorfuException("sync() failed ");
				}
			}
		}
	}
	
	/**
	 * trim a prefix of log up to the specified position
	 * 
	 * @param offset the position to trim to (exclusive)
	 * 
	 * @throws CorfuException
	 */
	public void trim(long offset) throws CorfuException {
        for (SegmentView s : CM.segmentlist) {
            if (offset <= s.getStartOff()) break;

            for (Vector<Endpoint> rset : s.getGroups()) {
                for (Endpoint ep : rset) {
                    if (ep == null) continue; // fault unit, removed from configuration
                    LogUnitService.Client sunit = getSUnitOf(ep);
                    try {
                        sunit.trim(new UnitServerHdr(CM.getEpoch(), offset));
                    } catch (TException e) {
                        throw new InternalCorfuException("trim() failed ");
                    }
                }
            }
        }
        // TODO proposeTrim(long offset);
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
		EntryLocation el = CM.getLocationForOffset(offset);
		LogUnitService.Client sunit = getSUnitOf(el.group.elementAt(0));
		try {
			return sunit.readmeta(new UnitServerHdr(CM.getEpoch(), offset));
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
	 * @see com.microsoft.corfu.DbgAPI#write(long, byte[])
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

    public LogUnitWrap rebuild(long offset) throws CorfuException {
        EntryLocation el = CM.getLocationForOffset(offset);
        LogUnitConfigService.Client cnfg = getCfgOf(el.group.elementAt(0));
        LogUnitWrap ret = null;
        try {
            ret = cnfg.rebuild();
        } catch (TException t) {
            throw new InternalCorfuException("rebuild failed");
        }
        return ret;
    }

    // from here down, implement ConfigAPI interface for administration:
    // ==========================================================

    public void sealepoch() throws CorfuException {
        for (Vector<Endpoint> grp : CM.getActiveSegmentView().getGroups())
            for (Endpoint ep : grp) {
                LogUnitConfigService.Client cnfg = getCfgOf(ep);
                try {
                    cnfg.epochchange(new UnitServerHdr(CM.getEpoch()+1, 0));
                } catch (TException e) {
                    throw new InternalCorfuException("sealepoch failed");
                }
            }
    }

    public void sealepoch(int gind, int excludeind) throws CorfuException {
        Vector<Endpoint> grp = CM.getGroupByNumber(gind);
        for (int rind = 0; rind < grp.size(); rind++) {
            if (rind == excludeind) continue;
            Endpoint ep = grp.elementAt(rind);
            if (ep == null) continue;
            LogUnitConfigService.Client cfg = getCfgOf(ep);
            try {
                cfg.epochchange(new UnitServerHdr(CM.getEpoch()+1, 0));

            } catch (TException e) {
                throw new InternalCorfuException("sealepoch failed");
            }
        }
    }

    public CorfuConfiguration pullConfig() throws CorfuException {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        CorfuConfiguration C = null;

        try {
            HttpGet httpget = new HttpGet("http://localhost:8000/corfu"); // TODO use 'master' for master-hostname

            System.out.println("Executing request: " + httpget.getRequestLine());
            HttpResponse response = httpclient.execute(httpget);

            log.info("pullConfig from master: {}" , response.getStatusLine());

            C = new CorfuConfiguration(response.getEntity().getContent());
        } catch (ClientProtocolException e) {
            throw new InternalCorfuException("cannot pull configuration");
        } catch (IOException e) {
            throw new InternalCorfuException("cannot pull configuration");
        }
        return C;
    }

    public void proposeRemoveUnit(int gind, int rind) throws CorfuException {
        sealepoch(gind, rind);

        proposeReconfig(CM.ConfToXMLString(
                CorfuConfiguration.CONFCHANGE.REMOVE,
                CM.getActiveSegmentView().getStartOff(),
                -1,
                gind,
                rind,
                null,
                -1
                ) );
    }

    public void proposeDeployUnit(int gind, int rind, String hostname, int port) throws CorfuException {
        proposeReconfig(CM.ConfToXMLString(
                CorfuConfiguration.CONFCHANGE.DEPLOY,
                CM.getActiveSegmentView().getStartOff(),
                -1, // TODO: later, use this to allow lazy recovery of deployed unit by sealing off a prefix of the segment
                gind,
                rind,
                hostname,
                port
        ) );
    }

    private void proposeReconfig(String proposal) throws CorfuException {

        DefaultHttpClient httpclient = new DefaultHttpClient();
        try {
            HttpPost httppost = new HttpPost("http://localhost:8000/corfu");
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
}