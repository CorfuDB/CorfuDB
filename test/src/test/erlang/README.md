# 1. Running the QuickCheck tests

Welcome to property-based testing of CorfuDB using QuickCheck.  Two
implementations of QuickCheck are supported:

* PropEr, the open source QuickCheck implementation in Erlang
* EQC, Quviq's commercial/closed source QuickCheck implementation in Erlang

Both require an Erlang/OTP runtime system installed on your machine.
You will also need Git and GNU Make.
See the "Prerequisites" section for details.

# 2. Prerequisites

We assume that you already have the development prerequisites
installed for Java development for CorfuDB: Java JDK version 8 and
also `maven`.

## 2.1 Erlang/OTP

I (Scott) recommend using Erlang/OTP version 17.  Later versions will
probably work, but I haven't tested them.  Please contact me if you're
trying to use a later version but run into difficulty.

### Linux: Debian & Ubuntu

Please: DO NOT USE THE DEFAULT ERLANG PACKAGING PROVIDED BY DEBIAN AND UBUNTU.

Instead, use a pre-compiled package from Erlang Solutions.

Pick _*one of*_ the following 32-bit or 64-bit package variable
definition and paste it into your shell.

    OTP_DEB=esl-erlang_17.5.3-1~ubuntu~precise_i386.deb
    OTP_DEB=esl-erlang_17.5.3-1~ubuntu~precise_amd64.deb

For other packages specifically compiled for other Debian and Ubuntu
releases, please see the Erlang Solutions
[Erlang/OTP download page](https://www.erlang-solutions.com/resources/download.html).

Then cut-and-paste the following.

    cd /tmp
    wget https://packages.erlang-solutions.com/erlang/esl-erlang/FLAVOUR_1_general/$OTP_DEB
    sudo dpkg -i --force-depends $OTP_DEB && rm -v $OTP_DEB

### Other Linux: RedHat, CentOS, etc.

Please see the Erlang Solutions
[Erlang/OTP download page](https://www.erlang-solutions.com/resources/download.html).
Download, then install using your OS's package manager.

### OS X

Download & install the OS X way using:

    https://packages.erlang-solutions.com/erlang/esl-erlang/FLAVOUR_1_general/esl-erlang_17.5.3~osx~10.10_amd64.dmg

### From source

A plain build (no fancy `configure` arguments) at 64 bits (preferred)
or 32 bits should be fine.  Source tarball link:

    http://erlang.org/download/otp_src_17.5.tar.gz

## 2.2 Git & make

If your machine doesn't have `git` & `make` installed, you'll need them.  Use
your OS package manager to install Git & GNU Make.

E.g, for Debian/Ubuntu, use these two commands:

    sudo apt-get -f install
    sudo apt-get -y install git make

(Command #1 overcomes any lingering dependency problems from
installing Erlang.)

## 2.3 PropEr, the open source QuickCheck property-based testing framework

Once Erlang/OTP version 17 is installed, adjust the "/path/to/" prefix
to suit your preferred location.  Then accordingly cut-and-paste this
into a shell to execute it:

    cd /path/to/some/dir
    git clone https://github.com/manopapad/proper.git
    cd proper
    make
    
## 2.3 Compiling CorfuDB

    cd /path/to/CorfudB
    mvn install

# 3. Running CorfuDB in QuickCheck testing mode

We usually start a single CorfuDB server process using the
`bin/corfu_server` script.
However, to run QuickCheck, we need to start the server via the
Clojure CLI shell.  Please add the following text to the file
`/tmp/start-corfu-server-with-quickcheck`:

    (def q-opts (new java.util.HashMap))
    (.put q-opts "<port>" "8000")
    (new org.corfudb.util.quickcheck.QuickCheckMode q-opts)
    (def server-args  "-l /tmp/some/path -s -d ERROR 8000")
    (org.corfudb.infrastructure.CorfuServer/main (into-array String (.split server-args " ")))

To start the corfu server, use:

    cat /tmp/start-corfu-server-with-quickcheck | java -Dlogback.configurationFile=./runtime/src/main/resources/logback.xml -cp `echo */target/*shaded.jar | sed 's/ /:/g'`:test/target/test-0.1-SNAPSHOT-tests.jar org.corfudb.shell.ShellMain

If you wish to change the server's command line arguments, adjust the
`server-args` string accordingly.

Flags are explained below:

* `-Q` for QuickCheck testing mode.  The CorfuDB "cmdlet" API is
  (ab)used by the QuickCheck framework for Erlang<->Java
  communication.
  Do not omit this flag.

* `-l /some/path` is very strongly recommended because we want to test
  stateful persistence with the file system.  In memory-only mode, we
  won't be able to test persistence, and also the `reboot` testing
  command will yield behavior that will confuse the QuickCheck model.
  Do not omit this flag.

* `-s` flag, to start the server in single-node mode.
  Do not omit this flag.

* `--cm-poll-interval=99999` can reduce the amount of spammy error
  messages on the server's console & log file by making the
  "configuration manager" polling interval very large.

* `8000` The TCP port number used by the server.  This port number
  must also be shared with QuickCheck.

    * Port 9000 is frequently used by (ahem) other CorfuDB
      developers.  Please use port 8000 when following these
      examples ... or else set the `CORFU_PORT` environment variable
      to match your CorfuDB server's `<port>` value.

# 4. Compiling and running the QuickCheck tests

The documentation here describes running the `layout_qc` test model,
which is a single server SUT (System Under Test) to exercise the Paxos
protocol primitive operations used by the CorfuDB layout manager.  The
method is very similar for testing other test models, e.g., the
`smrmap_qc` model for testing the state machine replicated map
`SMRMap`.

First, set an environment variable to point to the top of the local
PropEr source code repository.

    # Bash shell syntax
    export PROPER_DIR=/path/to/proper-repo-top
    
    # C shell syntax
    setenv PROPER_DIR /path/to/proper-repo-top

Next, compile the QuickCheck test model code.

    cd /path/to/CorfuDB-repo-top/src/test/erlang
    make clean
    ./Build.sh proper

Finally, start an Erlang REPL (read-eval-print-loop) shell.  This will
start an Erlang VM that is configured correctly: to access the PropEr
test code, to access our QuickCheck model code, and to access a
CorfuDB JVM that is located _on the same machine_ as the Erlang VM.

Note: Erlang will spit out weird error messages if you try to run more
      than one Erlang VM using the `Build.sh` script method below.
      For now, simply don't try to run more than one.  `^_^`

(It is certainly possible to put the Erlang VM and Java VM on
different machines, but doing it isn't automagic yet, sorry.)

    ./Build.sh proper-shell

... and you'll see a shell that looks like this:

    Erlang/OTP 17 [erts-6.4] [source] [64-bit] [smp:4:4] [async-threads:10] [hipe] [kernel-poll:false] [dtrace]
    
    Eshell V6.4  (abort with ^G)
    (qc@sbb5)1> 

Now we can run QuickCheck commands and support utility functions at
the Erlang shell.

If we can communicate correctly with the JVM, you should see this
value when calling the `sanity()` function:

    (qc@sbb5)1> layout_qc:sanity().
    ok

If the `sanity()` function throws an error, and you're sure that the
`corfu_server` process is running, then you probably need to add an
entry to `/etc/hosts` or to DNS.  The hostname that the QuickCheck
code is attempting to use can be shown by calling the function at
Erlang shell:

    (qc@sbb5)1> qc_java:local_endpoint_host().

The following example QuickCheck execution should run in under four
seconds.

    (qc@sbb5)3> proper:quickcheck(layout_qc:prop()).
    ....................................................................................................
    OK: Passed 100 test(s).
    
    29% {layout_qc,query,3}
    28% {layout_qc,prepare,4}
    18% {layout_qc,propose,5}
    9% {layout_qc,reset,2}
    7% {layout_qc,reboot,2}
    6% {layout_qc,commit,5}
    
    cmds_length
    minimum: 1
    average: 11.13
    maximum: 40
    true

Use the wrapper function `proper:numtests(NUM,...)` to change the
number of tests to be run by the inner function.  For example:

    proper:quickcheck(proper:numtests(5, layout_qc:prop_parallel())).
    proper:quickcheck(layout_qc:prop_parallel()).

NOTE: The parallel tests created by the `layout_qc:prop_parallel()`
      property will run far slower than the sequential-only tests of
      the `layout_qc:prop()` property.

The reference documentation for PropEr can be found at
[http://proper.softlab.ntua.gr/doc/](http://proper.softlab.ntua.gr/doc/).

# 5. Discussion of the Erlang<->Java communication method

## Erlang<->Java message passing

The CorfuDB JVM process, when running with the `corfu_server -Q`
flag, will run the Erlang distributed messaging protocol.  From the
network's point of view, the JVM *is* an Erlang VM.

The `Build.sh` script uses the following assumptions about the
CorfuDB SUT (System Under Test) and the Erlang VM.

* When the Erlang VM is started by `Build.sh proper-shell`, it
  will use an Erlang network node name such as `qc@foo`, where
  `foo` is probably the same as the UNIX hostname as shown by the
  `uname -n` command.  (For OS X, `uname -n` may display the name
  `foo.local`, depending on Mac's network configuration.)

* We assume that `foo` is a name that can be resolved to an IP
  address via `/etc/hosts` or by DNS.

* The CorfuDB endpoint TCP port number is assumed to be `X`.
In the examples given in this document, `X = 8000`.  If the
CorfuDB server is not using port 8000, please set the `CORFU_PORT`
environment variable before using `Build.sh`.

* The Erlang nodename that is used by the JVM is `corfu-PORT@foo`,
  where `PORT` is the CorfuDB server's TCP port number, and `foo`
  is the `uname -n` hostname (discussed above).

The JVM starts 16 extra Pthreads that each listen to an Erlang
mailbox.  These Erlang mailboxes are named `cmdlet0` through
`cmdlet15`.  Each worker Pthread will receive & process messages
from its mailbox serially in the order that the messages are
received.

The Erlang VM uses its standard message passing syntax to send
messages to the JVM, for example:

    {cmdlet4, 'corfu-8000@my-host'} ! {hello, "world"}.

The JVM sends replies back to the Erlang side in such a manner that
each Erlang handles them using the standard `receive` function.

## QuickCheck<->CorfuDB protocol operation

QuickCheck's command sequence generation does not *do* anything: it
just creates a list of stuff.  That stuff is a symbolic representation
of some SUT commands, e.g. `reset`, `query`, `prepare`, etc.

Another part of the QuickCheck framework will interpret a command
sequence to execute some Erlang functions (with the generated
arguments).  Here's where the RPC stuff comes into play.

As far as QuickCheck is concerned, plain old Erlang functions are
being executed.  However, each of those SUT functions are RPC stubs.
The stubs convert the Erlang function argments to strings; the strings
are exactly the command line argument syntax used by the CorfuDB
"cmdlet" programs.  The cmdlet program argument strings are send to
the remote JVM.

The CorfuDB server in `-Q` mode can run arbitrarily many cmdlet
invocations without exiting/restarting the JVM.  The return value of
the cmdlet (or its exception value, if an exception is thrown) is
converted to an ASCII representation (a Java `String[]` array) and
sent back to the Erlang side.

The structured ASCII value is structured enough to be parsed
unambiguously by the Erlang side to determine if the cmdlet's return
value matches the QuickCheck model.
