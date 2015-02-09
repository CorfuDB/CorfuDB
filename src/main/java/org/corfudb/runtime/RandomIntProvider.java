package org.corfudb.runtime;
import java.util.Random;

class RandomIntProvider implements TXListTester.RandomElementProvider<Integer> {
    Random r = new Random();
    public Integer randElem(Object i) {
        return new Integer(r.nextInt());
    }
}
class NonRandomIntProvider implements TXListTester.RandomElementProvider<Integer> {
    public Integer randElem(Object i) {
        return new Integer((Integer) i);
    }
}