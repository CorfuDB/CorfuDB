package org.corfudb.shell;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Created by mwei on 11/18/16.
 */
public class ShellMain {

    public static void main(String[] args) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("org.corfudb.shell"));
        IFn shell = Clojure.var("org.corfudb.shell", "-main");
        shell.invoke(args);
    }
}
