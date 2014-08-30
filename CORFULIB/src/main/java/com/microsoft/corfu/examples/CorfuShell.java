package com.microsoft.corfu.examples;

import com.microsoft.corfu.*;
import com.microsoft.corfu.CorfuConfiguration;
import java.lang.Override;
import java.lang.Runnable;
import java.lang.String;
import java.lang.Thread;

public class CorfuShell {

     /**
	 * @param args
	 */
	public static void main(String[] args) {
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				
                CommandParser prompt = new CommandParser();
                try {
                    prompt.Init("http://localhost:8000/corfu");
                } catch (CorfuException e) {
                    return;
                }
                prompt.Console();
			}}).run();
				
	}
}
