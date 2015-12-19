package org.corfudb;

import org.fusesource.jansi.Ansi;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/13/15.
 */
public class AbstractCorfuTest {

    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void succeeded(Description description) {
            System.out.print(ansi().a("[").fg(Ansi.Color.GREEN).a("PASS").reset().a("]").newline());
        }

        @Override
        protected void failed(Throwable e, Description description) {
            System.out.print(ansi().a("[").fg(Ansi.Color.RED).a("FAIL").reset().a("]").newline());
        }

        protected void starting(Description description) {
            System.out.print(String.format("%-60s", description.getMethodName()));
        }
    };

}
