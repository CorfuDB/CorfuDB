package org.corfudb.util.quickcheck;

import com.ericsson.otp.erlang.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class QuickCheckMode {
    private static OtpNode otpNode = null;
    private static final Object otpNodeLock = new Object();

    public QuickCheckMode(Map<String, Object> opts) {
        synchronized (otpNodeLock) {
            if (otpNode == null) {
                int port = Integer.parseInt((String) opts.get("<port>"));
                String nodename = "corfu-" + port;
                try {
                    otpNode = new OtpNode(nodename);

                    System.out.println("***************** Creating OtpNode Threads ************\n");
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
                    System.err.printf("Error creating OtpNode %s: %s\n", nodename, e.toString());
                }
            }
        }
    }

    private final int c0 = 0, c1 = 1, c2 = 2, c3 = 3, c4 = 4, c5 = 5, c6 = 6, c7 = 7, c8 = 8, c9 = 9,
            c10 = 10, c11 = 11, c12 = 12, c13 = 13, c14 = 14, c15 = 15;
    private void runErlMbox0() { runErlMbox(c0); }
    private void runErlMbox1() { runErlMbox(c1); }
    private void runErlMbox2() { runErlMbox(c2); }
    private void runErlMbox3() { runErlMbox(c3); }
    private void runErlMbox4() { runErlMbox(c4); }
    private void runErlMbox5() { runErlMbox(c5); }
    private void runErlMbox6() { runErlMbox(c6); }
    private void runErlMbox7() { runErlMbox(c7); }
    private void runErlMbox8() { runErlMbox(c8); }
    private void runErlMbox9() { runErlMbox(c9); }
    private void runErlMbox10() { runErlMbox(c10); }
    private void runErlMbox11() { runErlMbox(c11); }
    private void runErlMbox12() { runErlMbox(c12); }
    private void runErlMbox13() { runErlMbox(c13); }
    private void runErlMbox14() { runErlMbox(c14); }
    private void runErlMbox15() { runErlMbox(c15); }

    private void runErlMbox(int num) {
        Thread.currentThread().setName("DistErl-" + num);
        try {
            OtpMbox mbox = otpNode.createMbox("cmdlet" + num);

            OtpErlangObject o;
            OtpErlangTuple msg;
            OtpErlangPid from;

            while (true) {
                try {
                    o = mbox.receive();
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
                                // instead of a zero length string.
                                sopts[i] = "";
                            } else {
                                sopts[i] = ((OtpErlangString) cmd.elementAt(i)).stringValue();
                            }
                        }
                        String s0 = sopts[0];
                        String[] args = Arrays.copyOfRange(sopts, 1, sopts.length);
                        String[] res;
                        switch (sopts[0]) {
                            case "corfu_layout":
                                res = QCLayout.main(args);
                                break;
                            case "corfu_smrobject":
                                res = QCSMRobject.main(args);
                                break;
                            default:
                                res = QCUtil.replyErr("invocation error");
                                break;
                        }
                        OtpErlangObject[] reslist = new OtpErlangObject[res.length];
                        for (int i = 0; i < res.length; i++) {
                            reslist[i] = new OtpErlangString(res[i]);
                        }
                        OtpErlangList reply_reslist = new OtpErlangList(reslist);
                        OtpErlangTuple reply = new OtpErlangTuple(new OtpErlangObject[] { id, reply_reslist });
                        mbox.send(from, reply);
                    }
                } catch (Exception e) {
                    System.err.printf("runErlMbox interation error: %s\n", e.toString());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.printf("runErlMbox error: %s\n", e.toString());
        }
    }
}
