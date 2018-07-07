/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AbortTransactionOperation;
import com.mongodb.operation.CommitTransactionOperation;

import io.opencensus.common.Scope;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

final class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private enum TransactionState {
        NONE, IN, COMMITTED, ABORTED
    }

    private final MongoClientDelegate delegate;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;

    private static final Tracer TRACER = Tracing.getTracer();

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options,
                      final MongoClientDelegate delegate) {
        super(serverSessionPool, originator, options);
        this.delegate = delegate;
    }

    @Override
    public boolean hasActiveTransaction() {
        return transactionState == TransactionState.IN || (transactionState == TransactionState.COMMITTED && commitInProgress);
    }

    @Override
    public boolean notifyMessageSent() {
        if (hasActiveTransaction()) {
            boolean firstMessageInCurrentTransaction = !messageSentInCurrentTransaction;
            messageSentInCurrentTransaction = true;
            return firstMessageInCurrentTransaction;
        } else {
            if (transactionState == TransactionState.COMMITTED || transactionState == TransactionState.ABORTED) {
                cleanupTransaction(TransactionState.NONE);
            }
            return false;
        }
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", transactionState == TransactionState.IN || transactionState == TransactionState.COMMITTED);
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        notNull("transactionOptions", transactionOptions);
        if (transactionState == TransactionState.IN) {
            throw new IllegalStateException("Transaction already in progress");
        }
        if (transactionState == TransactionState.COMMITTED) {
            cleanupTransaction(TransactionState.IN);
        } else {
            transactionState = TransactionState.IN;
        }
        getServerSession().advanceTransactionNumber();
        this.transactionOptions = TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions());
        WriteConcern writeConcern = this.transactionOptions.getWriteConcern();
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated.  Transaction options write concern can not be null");
        }
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Transactions do not support unacknowledged write concern");
        }
    }

    @Override
    public void commitTransaction() {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        try {
            if (messageSentInCurrentTransaction) {
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                commitInProgress = true;
                delegate.getOperationExecutor().execute(new CommitTransactionOperation(transactionOptions.getWriteConcern()),
                        readConcern, this);
            }
        } finally {
            commitInProgress = false;
            transactionState = TransactionState.COMMITTED;
        }
    }


    @Override
    public void abortTransaction() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionImpl.abortTransaction").startScopedSpan();

        try {
            if (transactionState == TransactionState.ABORTED) {
                TRACER.getCurrentSpan().setStatus(Status.ABORTED.withDescription("Cannot call abortTransaction twice"));
                throw new IllegalStateException("Cannot call abortTransaction twice");
            }
            if (transactionState == TransactionState.COMMITTED) {
                TRACER.getCurrentSpan().setStatus(
                        Status.ABORTED.withDescription("Cannot call abortTransaction after calling commitTransaction"));
                throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
            }
            if (transactionState == TransactionState.NONE) {
                TRACER.getCurrentSpan().setStatus(Status.INVALID_ARGUMENT.withDescription("No transaction was started"));
                throw new IllegalStateException("There is no transaction started");
            }
            try {
                if (messageSentInCurrentTransaction) {
                    TRACER.getCurrentSpan().addAnnotation("Message sent in current transaction");
                    ReadConcern readConcern = transactionOptions.getReadConcern();
                    TRACER.getCurrentSpan().addAnnotation("Got readConcern");
                    if (readConcern == null) {
                        TRACER.getCurrentSpan().setStatus(
                                Status.INTERNAL.withDescription("Invariant violated. Transaction options read concern cannot be null"));
                        throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                    }
                    delegate.getOperationExecutor().execute(new AbortTransactionOperation(transactionOptions.getWriteConcern()),
                            readConcern, this);
                }
            } catch (Exception e) {
                // ignore errors
            } finally {
                TRACER.getCurrentSpan().setStatus(Status.OK.withDescription("Aborted"));
                cleanupTransaction(TransactionState.ABORTED);
            }
        } finally {
            ss.close();
        }
    }

    @Override
    public void close() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionImpl.close").startScopedSpan();

        try {
            if (transactionState == TransactionState.IN) {
                abortTransaction();
            }
        } finally {
            super.close();
            ss.close();
        }
    }

    private void cleanupTransaction(final TransactionState nextState) {
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
    }
}
