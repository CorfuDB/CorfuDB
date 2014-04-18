package com.microsoft.corfu.sequencer;

import com.microsoft.corfu.CorfuConfigManager;

import java.io.File;

/**
 * Created by dalia on 4/17/2014.
 */
public class CorfuSequencerMainDriver {
    static class dostats implements Runnable {

        CorfuSequencerTask CI;

        public dostats(CorfuSequencerTask CI) {
            super();
            this.CI = CI;
        }

        @Override
        public void run() {
            long starttime = System.currentTimeMillis();
            long elapsetime = 0;
            long lastpos = -1, newpos = -1;

            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                newpos = CI.pos.get();
                if (lastpos != newpos) {
                    elapsetime = System.currentTimeMillis() - starttime;
                    System.out.println("++stats: pos=" + newpos/1000 + "K elapse ~" + elapsetime/1000 + " seconds");
                    lastpos = newpos;
                }
            }
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        CorfuConfigManager CM = new CorfuConfigManager(new File("./corfu.xml"));

        int port = CM.getSequencer().getPort();
        CorfuSequencerTask.port = port;
        final CorfuSequencerTask CI = new CorfuSequencerTask();
        new Thread(new Runnable() {
            @Override
            public void run() {
                CI.serverloop();
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                new dostats(CI).run();
            }
        });
    }
}
