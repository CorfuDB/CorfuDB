package org.corfudb.cmdlets;

import lombok.extern.slf4j.Slf4j;
import com.ericsson.otp.erlang.*;

import java.io.IOException;
import java.util.Map;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

/**
 * Created by fritchie on 8/29/16.
 */
@Slf4j
public class QuickCheckMode {
    /**
     * TODO refactor/move to another class
     */
    private Thread erlNodeThread;

    /**
     * TODO refactor/move to another class
     */
    private static OtpNode otpNode = null;
    private static Object otpNodeLock = new Object();

    public QuickCheckMode(Map<String, Object> opts) {
        synchronized (otpNodeLock) {
            if (otpNode == null) {
                int port = Integer.parseInt((String) opts.get("<port>"));
                String nodename = "corfu-" + port;
                try {
                    otpNode = new OtpNode(nodename);

                    System.out.println("\n\n***************** Creating lots of OtpNode Threads ************\n\n");
                    Thread erlNodeThread0 = new Thread(this::runErlMbox0);
                    erlNodeThread0.start();
                    Thread erlNodeThread1 = new Thread(this::runErlMbox1);
                    erlNodeThread1.start();
                    Thread erlNodeThread2 = new Thread(this::runErlMbox2);
                    erlNodeThread2.start();
                    Thread erlNodeThread3 = new Thread(this::runErlMbox3);
                    erlNodeThread3.start();
                    Thread erlNodeThread4 = new Thread(this::runErlMbox4);
                    erlNodeThread4.start();
                    Thread erlNodeThread5 = new Thread(this::runErlMbox5);
                    erlNodeThread5.start();
                    Thread erlNodeThread6 = new Thread(this::runErlMbox6);
                    erlNodeThread6.start();
                    Thread erlNodeThread7 = new Thread(this::runErlMbox7);
                    erlNodeThread7.start();
                    Thread erlNodeThread8 = new Thread(this::runErlMbox8);
                    erlNodeThread8.start();
                    Thread erlNodeThread9 = new Thread(this::runErlMbox9);
                    erlNodeThread9.start();
                    Thread erlNodeThread10 = new Thread(this::runErlMbox10);
                    erlNodeThread10.start();
                    Thread erlNodeThread11 = new Thread(this::runErlMbox11);
                    erlNodeThread11.start();
                    Thread erlNodeThread12 = new Thread(this::runErlMbox12);
                    erlNodeThread12.start();
                    Thread erlNodeThread13 = new Thread(this::runErlMbox13);
                    erlNodeThread13.start();
                    Thread erlNodeThread14 = new Thread(this::runErlMbox14);
                    erlNodeThread14.start();
                    Thread erlNodeThread15 = new Thread(this::runErlMbox15);
                    erlNodeThread15.start();
                } catch (IOException e) {
                    log.info("Error creating OtpNode {}: {}", nodename, e);
                }
            }
        }
    }

    private void runErlMbox0() { runErlMbox(0); }
    private void runErlMbox1() { runErlMbox(1); }
    private void runErlMbox2() { runErlMbox(2); }
    private void runErlMbox3() { runErlMbox(3); }
    private void runErlMbox4() { runErlMbox(4); }
    private void runErlMbox5() { runErlMbox(5); }
    private void runErlMbox6() { runErlMbox(6); }
    private void runErlMbox7() { runErlMbox(7); }
    private void runErlMbox8() { runErlMbox(8); }
    private void runErlMbox9() { runErlMbox(9); }
    private void runErlMbox10() { runErlMbox(10); }
    private void runErlMbox11() { runErlMbox(11); }
    private void runErlMbox12() { runErlMbox(12); }
    private void runErlMbox13() { runErlMbox(13); }
    private void runErlMbox14() { runErlMbox(14); }
    private void runErlMbox15() { runErlMbox(15); }

    private void runErlMbox(int num) {
        Thread.currentThread().setName("DistErl-" + num);
        try {
            OtpMbox mbox = otpNode.createMbox("cmdlet" + num);

            OtpErlangObject o;
            OtpErlangTuple msg;
            OtpErlangPid from;
            CmdletRouter cr = new CmdletRouter();

            while (true) {
                try {
                    o = mbox.receive();
                    // System.err.print("{"); System.err.flush(); // Thread.sleep(100);
                    if (o instanceof OtpErlangTuple) {
                        msg = (OtpErlangTuple) o;
                        from = (OtpErlangPid) msg.elementAt(0);
                        OtpErlangObject id = msg.elementAt(1);
                        OtpErlangList cmd = (OtpErlangList) msg.elementAt(2);
                        String[] sopts = new String[cmd.elements().length];
                        for (int i = 0; i < sopts.length; i++) {
                            if (cmd.elementAt(i).getClass() == OtpErlangList.class) {
                                // We're expecting a string always, but
                                // the Erlang side will send an empty list
                                // for a zero length string.
                                sopts[i] = "";
                            } else {
                                sopts[i] = ((OtpErlangString) cmd.elementAt(i))
                                        .stringValue();
                            }
                        }
                        String[] res = cr.main2(sopts);
                        OtpErlangObject[] reslist = new OtpErlangObject[res.length];
                        for (int i = 0; i < res.length; i++) {
                            reslist[i] = new OtpErlangString(res[i]);
                        }
                        OtpErlangList reply_reslist = new OtpErlangList(reslist);
                        OtpErlangTuple reply = new OtpErlangTuple(new OtpErlangObject[] { id, reply_reslist });
                        mbox.send(from, reply);
                        // System.err.print("}"); System.err.flush();
                    }
                } catch (Exception e) {
                    log.warn("Bummer " + e);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            log.warn("Yo, bummer: " + e);
        }
    }
}
