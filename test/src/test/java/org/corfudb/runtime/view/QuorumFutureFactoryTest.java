/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by Konstantin Spirov on 2/6/2017.
 */
public class QuorumFutureFactoryTest {

    @Test
    public void testSingleFutureIncompleteComplete() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1);
        try {
            result.get(10, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        assertFalse(result.isDone());
        f1.complete("ok");
        Object value = result.get(100, TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        assertTrue(result.isDone());
    }

    @Test
    public void testInfiniteGet() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1);
        f1.complete("ok");
        Object value = result.get();
        assertEquals("ok", value);
        assertTrue(result.isDone());
    }


    @Test
    public void test2FuturesIncompleteComplete() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1, f2);
        try {
            result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        try {
            result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f1.complete("ok");
        Object value = result.get(100, TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
    }

    @Test
    public void test3FuturesIncompleteComplete() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        CompletableFuture<Object> f3 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1, f2, f3);
        try {
            result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        try {
            result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f3.complete("ok");
        Object value = result.get(100, TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        f1.complete("ok");
        value = result.get(10, TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
    }


    @Test
    public void test3FuturesWithFirstWinnerIncompleteComplete() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        CompletableFuture<Object> f3 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getFirstWinsFuture(f1, f2, f3);
        try {
            result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        Object value = result.get(100, TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
    }


    @Test
    public void testException() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1);
        f1.completeExceptionally(new NullPointerException());
        try {
            Object value = result.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            // expected behaviour
        }
        assertTrue(result.isDone());
    }

    @Test
    public void testCanceledFromInside() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1);
        f1.cancel(true);
        Object value = result.get(100, TimeUnit.MILLISECONDS);
        assertTrue(result.isCancelled());
        assertTrue(result.isDone());
        assertNull(value);
    }


    @Test
    public void testCanceledPlusException() throws Exception {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        Future<Object> result = QuorumFutureFactory.getQuorumFuture(f1, f2);
        f1.cancel(true);
        try {
            result.get(10, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.completeExceptionally(new NullPointerException());
        try {
            Object value = result.get(10, TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            // expected behaviour
        }
        assertTrue(result.isCancelled());
        assertTrue(result.isDone());
    }




}
