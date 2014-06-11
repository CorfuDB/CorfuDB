/**
 * @author dalia
 *
 * A simple demonstration of a Corfu client.
 * 
 * The main() procedure here appends two extents of different size to the log,
 * and then reads two extents from the head of the log.
 */
package com.microsoft.corfuapps;

import java.io.File;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.ExtntWrap;

public class Helloworld {

	public static void main(String[] args) {
		CorfuClientImpl crf;
		
		// establish client connection with Corfu service
		try {
			crf = new CorfuClientImpl();
		} catch (CorfuException e) {
			System.out.println("cannot establish connection to Corfu service, quitting");
			return;
		}

		// append to log
		try {
			int sz = crf.grainsize();			
			long offset = crf.appendExtnt(new byte[sz], sz);
			System.out.println("appended " + sz + " bytes to log at position " + offset);
			
			sz = 2*sz;
			offset = crf.appendExtnt(new byte[sz], sz);
			System.out.println("appended " + sz + " bytes to log at position " + offset);
		} catch (CorfuException e) {
			System.out.println("Corfu error in appendExtnt: " + e.er);
			e.printStackTrace();
		}

		// read from log
		try {
			ExtntWrap ret = crf.readExtnt();
			System.out.println("1st read from log Extnt: " + ret.getInf());
			ret = crf.readExtnt();
			System.out.println("2nd read from log Extnt: " + ret.getInf());
		} catch (CorfuException e) {
			System.out.println("Corfu error in readExtnt: " + e.er);
			e.printStackTrace();
		}

	}

}
