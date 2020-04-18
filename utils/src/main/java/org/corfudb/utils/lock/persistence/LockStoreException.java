/*
 ***********************************************************************
 * Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
 ***********************************************************************
 */
package org.corfudb.utils.lock.persistence;

/**
 * Exception thrown when there is error in a LockStore operation.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public class LockStoreException extends Exception {
    public LockStoreException(String message) {
        super(message);
    }

    public LockStoreException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
