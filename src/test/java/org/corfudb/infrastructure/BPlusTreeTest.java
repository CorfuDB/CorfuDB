package org.corfudb.infrastructure;

import org.junit.Test;
import java.util.Random;
import static org.junit.Assert.*;

/**
 * Created by taia on 6/24/15.
 */
public class BPlusTreeTest {

    @Test
    public void testInserts() {
        for (int fanout = 2; fanout <= 10; fanout++) {
            BPlusTree<Integer, Integer> bt = new BPlusTree(fanout);
            Random r = new Random();
            int MAX = 10000;
            int NUM = 100;
            int[] keys = new int[NUM];
            for (int i = 0; i < NUM; i++) {
                int next = r.nextInt(MAX);
                bt.insert(next, next);
                keys[i] = next;
            }


            for (int i = 0; i < NUM; i++) {
                assertNotNull(bt.get(keys[i]));
                assertEquals(bt.get(keys[i]).intValue(), keys[i]);
            }
        }
    }
}
