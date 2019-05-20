package org.corfudb.platform.core;

/**
 * Logical domain isolation separating two otherwise identical entity instances (through their
 * identity reference) from being identified as the same entity.
 *
 * @author jameschang
 * @since 2018-07-25
 */
@FunctionalInterface
public interface Namespace {

    Namespace GLOBAL = () -> "Global";

    String resolve();
}
