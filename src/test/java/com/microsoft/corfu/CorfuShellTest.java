package com.microsoft.corfu;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.ExtntWrap;

public class CorfuShellTest {

	static CorfuConfigManager CM = null;
	static CorfuClientImpl crf = null;
	static long offset = 0;
	
	interface helper {
		void helperf(long off) throws CorfuException ;
	}
	
	static helper printhelp = 
			new helper() {
			@Override
			public void helperf(long dummy) {
				System.out.println("------Usage:--------- ");
				for (Iterator<String> it = debugger.keySet().iterator(); it.hasNext(); ) {
					System.out.println(it.next());
				}
				for (Iterator<String> it = debugger2.keySet().iterator(); it.hasNext(); ) {
					System.out.println(it.next() + " <offset or length>");
				}
				System.out.println("-------------------- ");
			}};
	
	static HashMap<String, helper> debugger = new HashMap<String, helper>(){  // all commands with no params

		{
			put("help", printhelp);
			put("bounds",
					new helper() {
					@Override
					public void helperf(long dummy) throws CorfuException {
						long head, tail, ctail;
						
						head = crf.queryhead(); 
						tail = crf.querytail(); 
						System.out.println("Log[" + 
						head + ".." + tail + "]");
					}
				});
			
			put("dbg", 
					new helper() {
					@Override
					public void helperf(long dummy) throws CorfuException {
						ExtntWrap di;
						di = crf.dbg(offset);
						System.out.println("err=" + di.getErr());
						System.out.println("meta: " + di.getInf());
					}
				});
				
			put("read",
				new helper() {
				@Override
				public void helperf(long dummy) throws CorfuException {
					ExtntWrap ret;
					ret = crf.readExtnt(offset);
					System.out.println("read: size=" + ret.getCtntSize() + " meta="+ ret.getInf());
				}
			});
				
			put("trim",
					new helper() {
				@Override
				public void helperf(long dummy) throws CorfuException {
					crf.trim(offset);
				}
			});
			
			put("fix",
					new helper() {
					@Override
					public void helperf(long dummy) throws CorfuException {
						ExtntWrap ret;
						if (offset > 0) {
							System.out.println("setting reader mark to " + (offset-1));
							ret = crf.readExtnt(offset-1);
							System.out.println("read: size=" + ret.getCtntSize() + " meta="+ ret.getInf());
						}
						crf.repairNext();
					}
			});			
			
			put("quit", 
					new helper() {
					@Override
					public void helperf(long dummy) { System.exit(0); }
			});
		}};
		
	static HashMap<String, helper> debugger2 = new HashMap<String, helper>(){ // all commands with one offset-param

		{
			put("seek", 
					new helper() {
					@Override
					public void helperf(long off) throws CorfuException {
						offset = off;
					} 
			});
			
			put("append",
				new helper() {
				public void helperf(long length) throws CorfuException {
					long off = crf.appendExtnt(new byte[(int)length], (int)length);
					System.out.println("appended " + length + " bytes at " + off);
				}
			});
			
			put("write",
					new helper() {
					public void helperf(long length) throws CorfuException {
						crf.write(offset,  new byte[(int)length]);
						System.out.println("wrote " + length + " bytes at " + offset);
					}
					
				});
	}};
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		new Thread(new Runnable() {
			@Override
			public void run() {

				BufferedReader c;
				long off = -1; 
				boolean hasparam = false;
				helper h;

				try {
					CM = new CorfuConfigManager(new File("./0.aux"));
					crf = new CorfuClientImpl(CM);
				} catch (CorfuException e) {
					e.printStackTrace();
					return;
				}
		
				c = new BufferedReader(new InputStreamReader(System.in));
				for (;;) {
					try {
						long head, tail;
						
						head = crf.queryhead(); 
						tail = crf.querytail(); 
						System.out.print("Log[" + 
						head + ".." + tail + "] ");
					} catch (CorfuException ce) {
						System.out.println("boundaries check failed");
						System.out.println("Corfu err: " + ce.er);
						ce.printStackTrace();
					}
					
					StringTokenizer I;
					try { I = new StringTokenizer(c.readLine()); } catch (Exception ie) { return; }
					if (!I.hasMoreTokens()) continue;
					
					String cmd = I.nextToken();
					if (I.hasMoreTokens()) {
						hasparam = true;
						String OffStr = I.nextToken();
						off = Long.parseLong(OffStr);
						h = debugger2.get(cmd);
					} else {
						hasparam = false;
						h = debugger.get(cmd);
					}
				
					if (h == null) {
						System.out.println("unrecognized command!");
					
						h = debugger.get("help");
						try { if (h != null) h.helperf(off); } catch (CorfuException dummye) {}
						continue;
					}

					try {
						h.helperf(off);
					} catch (CorfuException ce) {
						System.out.println("call failed (off=" + off + ")");
						System.out.println("Corfu err: " + ce.er);
						ce.printStackTrace();
					}
				}
		
			}
		}).run();
				
	}
}
