package com.microsoft.corfu.unittests;

import java.io.File;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.ExtntWrap;

// A simple demonstration of a Corfu client.
//
// The main() procedure here appends one entry to the log and then reads one entry from the head of the log.
//
public class Helloworld {

	public static void main(String[] args) {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));
		CorfuClientImpl crf;
		
		// establish client connection with Corfu service
		try {
			crf = new CorfuClientImpl(CM);
		} catch (CorfuException e) {
			System.out.println("cannot establish connection to Corfu service, quitting");
			return;
		}

		try {
			int sz = 2*CM.getGrain();
			
			long offset = crf.appendExtnt(new byte[sz], sz);
			System.out.println("appended " + sz + " bytes to log at position " + offset);
			offset = crf.appendExtnt(new byte[sz], sz);
			System.out.println("appended " + sz + " bytes to log at position " + offset);
		} catch (CorfuException e) {
			System.out.println("Corfu error in appendExtnt: " + e.er);
			e.printStackTrace();
		}

		try {
			ExtntWrap ret = crf.readExtnt();
			System.out.println("read from log Extnt: " + ret.getInf());
			ret = crf.readExtnt();
			System.out.println("read from log Extnt: " + ret.getInf());
		} catch (CorfuException e) {
			System.out.println("Corfu error in readExtnt: " + e.er);
			e.printStackTrace();
		}

	}

}
