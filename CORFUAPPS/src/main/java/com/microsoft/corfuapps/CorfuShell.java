package com.microsoft.corfuapps;

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

public class CorfuShell {

	static CorfuConfigManager CM = null;
	CorfuClientImpl crf = null;

	interface helper {
		void helperf(long[] args) throws CorfuException ;
	}
	
	helper printhelp = 
			new helper() {
			@Override
			public void helperf(long[] dummy) {
				System.out.println("-- alias (full-cmd) params.. : ----- synopsis ------- ");
				for (Iterator<String> it = alias.keySet().iterator(); it.hasNext(); ) {
					String cmdname = it.next();
                    String aliasname = alias.get(cmdname);
                    System.out.print("   " + aliasname  + "   (" + cmdname + ") ");
					info inf = infos.get(aliasname);
					for (int i = 1; i <= inf.nparams; i++) System.out.print(" <param" + i + ">");
					System.out.print("  -- ");
					System.out.println("    " + inf.help);
				}
                System.out.println("");
                System.out.println("all commands can be repeated with 'rpt <n> cmd args...'");
				System.out.println("-------------------- ");
			}};
	
	HashMap<String, helper> debugger = new HashMap<String, helper>();
	HashMap<String, String> alias = new HashMap<String, String>();
	class info {
		String help;
		int nparams;
		public info(String help, int nparams) {
			super();
			this.help = help;
			this.nparams = nparams;
		}
	}
	HashMap<String, info> infos = new HashMap<String, info>();
	
	void Init() {
			debugger.put("h", printhelp);  alias.put("help",  "h");
						infos.put("h",  new CorfuShell.info("print help menu", 0));
			
			debugger.put("b",
					new helper() {
					@Override
					public void helperf(long[] dummy) throws CorfuException {
						long head, tail;
						
						head = crf.queryhead(); 
						tail = crf.querytail(); 
						System.out.println("Log[" + 
						head + ".." + tail + "]");
					}
				});							alias.put("bounds", "b");
						infos.put("b",  new CorfuShell.info("print log boundaries", 0));
			
			debugger.put("d", 
					new helper() {
					@Override
					public void helperf(long[] args) throws CorfuException {
						ExtntWrap di;
						di = crf.dbg(args[0]);
						System.out.println("err=" + di.getErr());
						System.out.println("meta: " + di.getInf());
					}
				});						alias.put("debug",  "d");
						infos.put("d",  new CorfuShell.info("print meta info for offset ", 1));
				
			debugger.put("ra",
				new helper() {
				@Override
				public void helperf(long[] args) throws CorfuException {
					ExtntWrap ret;
					ret = crf.readExtnt(args[0]);
					System.out.println("read: size=" + ret.getCtntSize() + " meta="+ ret.getInf());
				}
			});							alias.put("readat",  "ra");
						infos.put("ra",  new CorfuShell.info("read entry at offset", 1));
				
			debugger.put("rn",
					new helper() {
					public void helperf(long[] dummy) throws CorfuException {
						ExtntWrap ret;
						ret = crf.readExtnt();
						System.out.println("read: size=" + ret.getCtntSize() + " meta="+ ret.getInf());
					}
				});						alias.put("readnext",  "rn");
						infos.put("rn",  new CorfuShell.info("read next log entry", 0));
				
			debugger.put("t",
					new helper() {
				@Override
				public void helperf(long[] args) throws CorfuException {
					crf.trim(args[0]);
				}
			});							alias.put("trim",  "t");
						infos.put("t",  new CorfuShell.info("trim log to offset ", 1));

			debugger.put("q", 
					new helper() {
					@Override
					public void helperf(long[] dummy) { System.exit(0); }
			});							alias.put("quit",  "q");
						infos.put("q",  new CorfuShell.info("quit corfu shell", 0));
		
			debugger.put("a",
				new helper() {
				public void helperf(long[] args) throws CorfuException {
					long off = crf.appendExtnt(new byte[(int)args[0]], (int)args[0]);
					System.out.println("appended " + args[0] + " bytes at " + off);
				}
			});							alias.put("append",  "a");
						infos.put("a",  new CorfuShell.info("append extent of specified length to log", 1));
			
			debugger.put("w",
					new helper() {
					public void helperf(long[] args) throws CorfuException {
						crf.write(args[0],  new byte[(int)args[1]]);
						System.out.println("wrote " + args[1] + " bytes at " + args[0]);
					}
					
				});						alias.put("writeat",  "w");
						infos.put("w",  new CorfuShell.info("write extent of specified length to log", 2));

            debugger.put("tkn",
                new helper() {
                    public void helperf(long[] args) throws CorfuException {
                        crf.tokenserverrecover(args[0]);
                        System.out.println("token server set at  " + args[0]);
                    }

                });						alias.put("tokenrecover",  "tkn");
                        infos.put("tkn",  new CorfuShell.info("recover token server position to the specified", 1));

		BufferedReader c;
		long off = -1; 
		boolean hasparam = false;
		helper h;
		info inf;

		try {
			crf = new CorfuClientImpl();
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
			int rpt = 1;
            if (cmd.equals("rpt")) {
                if (!I.hasMoreTokens()) {
                    BadFormat("missing repeat count and command");
                    continue;
                }
                rpt = Integer.valueOf(I.nextToken());
                if (!I.hasMoreTokens()) {
                    BadFormat("missing command");
                    continue;
                }
                cmd = I.nextToken();
            }

			// look up command
			h = debugger.get(cmd);
			if (h == null) { 			// check if using full command?
				cmd = alias.get(cmd); 
				if (cmd != null) h = debugger.get(cmd);
			}
			if (h == null) {
				BadFormat("unrecognized command!");
				continue;
			}
			inf = infos.get(cmd);
			
			// get params
            long[] args = new long[inf.nparams]; int j;
			for (j = 0; j < inf.nparams; j++) {
				if (!I.hasMoreTokens()) break;
				String OffStr = I.nextToken();
				args[j] = Long.parseLong(OffStr);
			}
            if (j < inf.nparams) {
                BadFormat("this command requires a parameter!");
                continue;
            }

			// dispatch command
			try {
                for (int r = 0; r < rpt; r++)
    				h.helperf(args);
			} catch (CorfuException ce) {
				System.out.println("call failed (off=" + off + ")");
				System.out.println("Corfu err: " + ce.er);
				ce.printStackTrace();
			}
		}
	}
	
    void BadFormat(String msg) {
        System.out.println(msg);
        helper h = debugger.get("h");
        try { if (h != null) h.helperf(null); } catch (CorfuException dummye) {}
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				
                new CorfuShell().Init();
			}}).run();
				
	}
}
