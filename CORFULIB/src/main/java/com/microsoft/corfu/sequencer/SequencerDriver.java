/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.corfu.sequencer;

/**
 * Created by dalia on 4/17/2014.
 */
public class SequencerDriver {
    static class dostats implements Runnable {

        SequencerTask CI;

        public dostats(SequencerTask CI) {
            super();
            this.CI = CI;
        }

        @Override
        public void run() {
            System.out.println("stats thread started");
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

        int port = Integer.valueOf(args[0]);
        SequencerTask.port = port;
        final SequencerTask CI = new SequencerTask();
        System.out.println("sequencer started with port " + port);
        new Thread(new Runnable() {
            @Override
            public void run() {
                CI.serverloop();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                new dostats(CI).run();
            }
        }).start();
    }
}
