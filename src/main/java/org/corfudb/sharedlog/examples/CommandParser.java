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
package org.corfudb.sharedlog.examples;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.infrastructure.thrift.SimpleLogUnitWrap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Exception;import java.lang.Long;import java.lang.Override;import java.lang.String;import java.lang.System;import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.UnitServerHdr;


/**
 * Created by dalia on 7/1/2014.
 */
public class CommandParser {

    ClientLib crf = null;

    interface helper {
        void helperf(long[] args) throws CorfuException;
    }

    helper printhelp =
            new helper() {
                @Override
                public void helperf(long[] dummy) {
                    System.out.println("@C@ -- alias (full-cmd) params.. : ----- synopsis ------- ");
                    for (Iterator<String> it = alias.keySet().iterator(); it.hasNext(); ) {
                        String cmdname = it.next();
                        String aliasname = alias.get(cmdname);
                        System.out.print("   " + aliasname + "   (" + cmdname + ") ");
                        info inf = infos.get(aliasname);
                        for (int i = 1; i <= inf.nparams; i++) System.out.print(" <param" + i + ">");
                        System.out.print("  -- ");
                        System.out.println("@C@     " + inf.help);
                    }
                    System.out.println("@C@ ");
                    System.out.println("@C@ all commands can be repeated with 'rpt <n> cmd args...'");
                    System.out.println("@C@ -------------------- ");
                }
            };

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

    public void Init(final String masteraddress) throws CorfuException {
        debugger.put("h", printhelp);
        alias.put("help", "h");
        infos.put("h", new CommandParser.info("print help menu", 0));

        debugger.put("b",
                new helper() {
                    @Override
                    public void helperf(long[] dummy) throws CorfuException {
                        long head, tail;

                        head = crf.queryhead();
                        tail = crf.querytail();
                        System.out.println("@C@ Log[" +
                                head + ".." + tail + "]");
                    }
                }
        );
        alias.put("bounds", "b");
        infos.put("b", new CommandParser.info("print log boundaries", 0));

        debugger.put("d",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        ExtntWrap di;
                        di = crf.dbg(args[0]);
                        System.out.println("@C@ err=" + di.getErr());
                        System.out.println("@C@ meta: " + di.getInf());
                    }
                }
        );
        alias.put("debug", "d");
        infos.put("d", new CommandParser.info("print meta info for offset ", 1));

        debugger.put("bu",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        SimpleLogUnitWrap ret;
                        ret = crf.rebuild(args[0]);
                        System.out.println("@C@ rebuild: lowwater=" + ret.getLowwater() + " highwater=" + ret.getHighwater());
                    }
                }
        );
        alias.put("rebuild", "bu");
        infos.put("bu", new CommandParser.info("invoke rebuilt from the unit holding the specific offset", 1));

        debugger.put("cfg",
                new helper() {
                    @Override
                    public void helperf(long[] dummy) throws CorfuException {
                        crf.pullConfig(masteraddress);
                    }
                }
        );
        alias.put("pullconfig", "cfg");
        infos.put("cfg", new CommandParser.info("pull configuration", 0));

        debugger.put("rmunit",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        crf.proposeRemoveUnit((int) (args[0]), (int) (args[1]));
                        System.out.println("@C@ removed unit" + args[0] + "," + args[1]);
                    }
                }
        );
        alias.put("removeunit", "rmunit");
        infos.put("rmunit", new CommandParser.info("remove the specified logging unit", 2));

        debugger.put("kill",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        crf.killUnit((int) (args[0]), (int) (args[1]));
                        System.out.println("@C@ kill  unit" + args[0] + "," + args[1]);
                    }
                }
        );
        alias.put("killunit", "kill");
        infos.put("kill", new CommandParser.info("kill the specified logging unit", 2));

        debugger.put("addunit",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        crf.proposeDeployUnit((int) (args[0]), (int) (args[1]), "localhost", (int) (args[2]));
                        System.out.println("@C@ deployed unit localhost:" + args[2] + " at " + args[0] + "," + args[1]);
                    }
                }
        );
        alias.put("addunit", "addunit");
        infos.put("addunit", new CommandParser.info("add the specified logging unit", 3));

        debugger.put("ra",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        ExtntWrap ret;
                        ret = crf.readExtnt(args[0]);
                        System.out.println("@C@ read: size=" + ret.getCtntSize() + " meta=" + ret.getInf());
                    }
                }
        );
        alias.put("readat", "ra");
        infos.put("ra", new CommandParser.info("read entry at offset", 1));

        debugger.put("rn",
                new helper() {
                    public void helperf(long[] dummy) throws CorfuException {
                        ExtntWrap ret;
                        ret = crf.readExtnt();
                        System.out.println("@C@ read: size=" + ret.getCtntSize() + " meta=" + ret.getInf());
                    }
                }
        );
        alias.put("readnext", "rn");
        infos.put("rn", new CommandParser.info("read next log entry", 0));

        debugger.put("t",
                new helper() {
                    @Override
                    public void helperf(long[] args) throws CorfuException {
                        crf.trim(args[0]);
                    }
                }
        );
        alias.put("trim", "t");
        infos.put("t", new CommandParser.info("trim log to offset ", 1));

        debugger.put("q",
                new helper() {
                    @Override
                    public void helperf(long[] dummy) {
                        System.exit(0);
                    }
                }
        );
        alias.put("quit", "q");
        infos.put("q", new CommandParser.info("quit corfu shell", 0));

        debugger.put("a",
                new helper() {
                    public void helperf(long[] args) throws CorfuException {
                        long off = crf.appendExtnt(new byte[(int) args[0]], (int) args[0]);
                        System.out.println("@C@ appended " + args[0] + " bytes at " + off);
                    }
                }
        );
        alias.put("append", "a");
        infos.put("a", new CommandParser.info("append extent of specified length to log", 1));

        debugger.put("w",
                new helper() {
                    public void helperf(long[] args) throws CorfuException {
                        crf.write(args[0], new byte[(int) args[1]]);
                        System.out.println("@C@ wrote " + args[1] + " bytes at " + args[0]);
                    }

                }
        );
        alias.put("writeat", "w");
        infos.put("w", new CommandParser.info("write extent of specified length to log", 2));

        debugger.put("tkn",
                new helper() {
                    public void helperf(long[] args) throws CorfuException {
                        crf.tokenserverrecover(args[0]);
                        System.out.println("@C@ token server set at  " + args[0]);
                    }

                }
        );
        alias.put("tokenrecover", "tkn");
        infos.put("tkn", new CommandParser.info("recover token server position to the specified", 1));

        if (masteraddress != null) {
            try {
                crf = new ClientLib(masteraddress);
            } catch (CorfuException e) {
                e.printStackTrace();
                throw new CorfuException("cannot initalize command parser with master " + masteraddress);
            }
        }
    }

    public void Console() {
        BufferedReader c;

        c = new BufferedReader(new InputStreamReader(System.in));
        for (;;) {
            try {
                long head, tail;

                head = crf.queryhead();
                tail = crf.querytail();
                System.out.print("Log[" +
                        head + ".." + tail + "] ");
                EvalCmd(c.readLine());
            } catch (CorfuException ce) {
                System.out.println("@C@ boundaries check failed");
                System.out.println("@C@ Corfu err: " + ce.er);
                ce.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void EvalCmd(String ln) {

        if (crf == null) {
            System.out.println("@C@ CmdParser not initialized!!!!!");
            return;
        }

        long off = -1;
        helper h;
        info inf;

        StringTokenizer I;
        try { I = new StringTokenizer(ln); } catch (Exception ie) { return; }
        if (!I.hasMoreTokens()) return;

        String cmd = I.nextToken();
        int rpt = 1;
        if (cmd.equals("rpt")) {
            if (!I.hasMoreTokens()) {
                BadFormat("missing repeat count and command");
                return;
            }
            rpt = java.lang.Integer.valueOf(I.nextToken());
            if (!I.hasMoreTokens()) {
                BadFormat("missing command");
                return;
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
            return;
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
            return;
        }

        // dispatch command
        try {
            for (int r = 0; r < rpt; r++)
                h.helperf(args);
        } catch (CorfuException ce) {
            System.out.println("@C@ call failed (off=" + off + ")");
            System.out.println("@C@ Corfu err: " + ce.er);
            ce.printStackTrace();
        }
    }

    public void BadFormat(String msg) {
        System.out.println(msg);
        helper h = debugger.get("h");
        try {
            if (h != null) h.helperf(null);
        } catch (CorfuException dummye) {
        }
    }
}
