package com.microsoft.corfu.unittests;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuLogMark;
import com.microsoft.corfu.ExtntInfo;
import com.microsoft.corfu.ExtntWrap;

public class SUnitDbg {

	static CorfuConfigManager CM = null;
	static CorfuClientImpl crf = null;
	
	static void printhelp() {
		System.out.println("commands: ");
		System.out.println("help");
		System.out.println("read <offset>");
		System.out.println("bounds");
		System.out.println("quit");
	}

	static void readhelper(String OffStr) {
		long off = Long.parseLong(OffStr);
		ExtntInfo inf;
		try {
			inf = crf.dbg(off);
			System.out.println("read: " + inf);
		} catch (CorfuException e) {
			System.out.println("readExtnt failed off=" + off);
			e.printStackTrace();
		}
	}
	
	static void boundshelper() {
		long head, tail, ctail;
		
		try {
			head = crf.checkLogMark(CorfuLogMark.HEAD); 
			tail = crf.checkLogMark(CorfuLogMark.TAIL); 
			ctail = crf.checkLogMark(CorfuLogMark.CONTIG); 
			System.out.println("Log boundaries (head, contiguous-tail, tail): (" + 
			head + ", " + ctail + ", " + tail + ")");
		} catch (CorfuException e) {
			System.out.println("checkLogMark failed");
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader c;

		try {
			CM = new CorfuConfigManager(new File("./0.aux"));
			crf = new CorfuClientImpl(CM);

			c = new BufferedReader(new InputStreamReader(System.in));
			for (;;) {
				System.out.print("> ");
				// System.out.flush();
				
				StringTokenizer I = new StringTokenizer(c.readLine());
				if (!I.hasMoreTokens()) continue;
				
				String cmd = I.nextToken();
				if (cmd.startsWith("help")) {
					printhelp();
				} 
				
				else if (cmd.startsWith("read")) {
					if (!I.hasMoreTokens()) {
						printhelp();
						continue;
					}
					String OffStr = I.nextToken();
					readhelper(OffStr);
				}
				
				else if (cmd.startsWith("bound")) {
					boundshelper();
				}
				
				else if (cmd.startsWith("quit")) {
					break;
				}
			}

		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ;
		}
		
	}

}
