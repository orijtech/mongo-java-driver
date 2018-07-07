/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.selector.ServerSelector;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class MongoClientDelegate {
    private final Cluster cluster;
    private final ServerSessionPool serverSessionPool;
    private final List<MongoCredential> credentialList;
    private final Object originator;
    private final OperationExecutor operationExecutor;
    private static final Tracer TRACER = Tracing.getTracer();

    public MongoClientDelegate(final Cluster cluster, final List<MongoCredential> credentialList, final Object originator) {
        this(cluster, credentialList, originator, null);
    }

    public MongoClientDelegate(final Cluster cluster, final List<MongoCredential> credentialList, final Object originator,
                               @Nullable final OperationExecutor operationExecutor) {
        this.cluster = cluster;
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.credentialList = credentialList;
        this.originator = originator;
        this.operationExecutor = operationExecutor == null ? new DelegateOperationExecutor() : operationExecutor;
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    @Nullable
    public ClientSession createClientSession(final ClientSessionOptions options, final ReadConcern readConcern,
                                             final WriteConcern writeConcern, final ReadPreference readPreference) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientDelegate.ClientSession.createClientSession")
                         .startScopedSpan();

        try {
            notNull("readConcern", readConcern);
            notNull("writeConcern", writeConcern);
            notNull("readPreference", readPreference);

            if (credentialList.size() > 1) {
                return null;
            }

            ClusterDescription connectedClusterDescription = getConnectedClusterDescription();

            if (connectedClusterDescription.getType() == ClusterType.STANDALONE
                    || connectedClusterDescription.getLogicalSessionTimeoutMinutes() == null) {
                return null;
            } else {
                ClientSessionOptions mergedOptions = ClientSessionOptions.builder(options)
                        .defaultTransactionOptions(
                                TransactionOptions.merge(
                                        options.getDefaultTransactionOptions(),
                                        TransactionOptions.builder()
                                                .readConcern(readConcern)
                                                .writeConcern(writeConcern)
                                                .readPreference(readPreference)
                                                .build()))
                        .build();
                return new ClientSessionImpl(serverSessionPool, originator, mergedOptions, this);
            }
        } finally {
            ss.close();
        }
    }

    public List<ServerAddress> getServerAddressList() {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (final ServerDescription cur : cluster.getDescription().getServerDescriptions()) {
            serverAddresses.add(cur.getAddress());
        }
        return serverAddresses;
    }

    public void close() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientDelegate.close").startScopedSpan();

        try {
            TRACER.getCurrentSpan().addAnnotation("Closing serverSessionPool");
            serverSessionPool.close();
            TRACER.getCurrentSpan().addAnnotation("Closing the cluster");
            cluster.close();
        } finally {
            ss.close();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    private ClusterDescription getConnectedClusterDescription() {
        Scope ss = TRACER.spanBuilder(
                "com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getConnectedClusterDescription")
                .startScopedSpan();

        try {
            ClusterDescription clusterDescription = cluster.getDescription();
            if (getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()) {
                cluster.selectServer(new ServerSelector() {
                    @Override
                    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                        return getServerDescriptionListToConsiderForSessionSupport(clusterDescription);
                    }
                });
                clusterDescription = cluster.getDescription();
            }
            return clusterDescription;
        } finally {
            ss.close();
        }
    }

    @SuppressWarnings("deprecation")
    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        Scope ss = TRACER.spanBuilder(
                "com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getServerDescriptionToConsiderForSessionSupport")
                .startScopedSpan();

        try {
            if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
                return clusterDescription.getAny();
            } else {
                return clusterDescription.getAnyPrimaryOrSecondary();
            }
        } finally {
            ss.close();
        }
    }

    private class DelegateOperationExecutor implements OperationExecutor {
        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern) {
            return execute(operation, readPreference, readConcern, null);
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern) {
            return execute(operation, readConcern, null);
        }

        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                             @Nullable final ClientSession session) {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.execute/readOperation")
                             .startScopedSpan();

            TRACER.getCurrentSpan().addAnnotation("Getting client session");
            ClientSession actualClientSession = getClientSession(session);
            TRACER.getCurrentSpan().addAnnotation("Getting readBinding");
            ReadBinding binding = getReadBinding(readPreference, readConcern, actualClientSession,
                    session == null && actualClientSession != null);
            try {
                if (session != null && session.hasActiveTransaction() && !binding.getReadPreference().equals(primary())) {
                    throw new MongoClientException("Read preference in a transaction must be primary");
                }
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(session, e);
                TRACER.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.toString()));
                throw e;
            } finally {
                binding.release();
                ss.close();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern, @Nullable final ClientSession session) {
            Scope ss = TRACER.spanBuilder(
                    "com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.execute/writeOperation")
                    .startScopedSpan();

            TRACER.getCurrentSpan().addAnnotation("Getting client session");
            ClientSession actualClientSession = getClientSession(session);
            TRACER.getCurrentSpan().addAnnotation("Getting writeBinding");
            WriteBinding binding = getWriteBinding(readConcern, actualClientSession, session == null && actualClientSession != null);

            try {
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(session, e);
                TRACER.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.toString()));
                throw e;
            } finally {
                TRACER.getCurrentSpan().addAnnotation("Invoking binding.release");
                binding.release();
                TRACER.getCurrentSpan().addAnnotation("Finished invoking binding.release");
                ss.close();
            }
        }

        WriteBinding getWriteBinding(final ReadConcern readConcern, @Nullable final ClientSession session, final boolean ownsSession) {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getWriteBinding")
                             .startScopedSpan();

            try {
                TRACER.getCurrentSpan().addAnnotation("Getting write binding");
                TRACER.getCurrentSpan().putAttribute("ownsSession", AttributeValue.booleanAttributeValue(ownsSession));
                return getReadWriteBinding(primary(), readConcern, session, ownsSession);
            } finally {
                ss.close();
            }

        }

        ReadBinding getReadBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                   @Nullable final ClientSession session, final boolean ownsSession) {
            Scope ss = TRACER.spanBuilder(
                    "com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getReadBinding")
                    .startScopedSpan();

            try {
                TRACER.getCurrentSpan().addAnnotation("Getting read binding");
                TRACER.getCurrentSpan().putAttribute("ownsSession", AttributeValue.booleanAttributeValue(ownsSession));
                return getReadWriteBinding(readPreference, readConcern, session, ownsSession);
            } finally {
                ss.close();
            }
        }

        ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                             @Nullable final ClientSession session, final boolean ownsSession) {
            Scope ss = TRACER.spanBuilder(
                    "com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getReadWriteBinding")
                    .startScopedSpan();

            try {
                TRACER.getCurrentSpan().addAnnotation("Getting readWrite binding from ClusterBinding");
                TRACER.getCurrentSpan().putAttribute("ownsSession", AttributeValue.booleanAttributeValue(ownsSession));
                ReadWriteBinding readWriteBinding = new ClusterBinding(cluster,
                                                                getReadPreferenceForBinding(readPreference, session), readConcern);

                if (session != null) {
                    TRACER.getCurrentSpan().putAttribute("null session", AttributeValue.booleanAttributeValue(true));
                    readWriteBinding = new ClientSessionBinding(session, ownsSession, readWriteBinding);
                } else {
                    TRACER.getCurrentSpan().putAttribute("null session", AttributeValue.booleanAttributeValue(false));
                }
                return readWriteBinding;
            } finally {
                ss.close();
            }
        }

        private void labelException(final @Nullable ClientSession session, final MongoException e) {
            if ((e instanceof MongoSocketException || e instanceof MongoTimeoutException)
                    && session != null && session.hasActiveTransaction() && !e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                e.addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
        }

        private ReadPreference getReadPreferenceForBinding(final ReadPreference readPreference, @Nullable final ClientSession session) {
            if (session == null) {
                return readPreference;
            }
            if (session.hasActiveTransaction()) {
                ReadPreference readPreferenceForBinding = session.getTransactionOptions().getReadPreference();
                if (readPreferenceForBinding == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read preference can not be null");
                }
                return readPreferenceForBinding;
            }
            return readPreference;
        }

        @Nullable
        ClientSession getClientSession(@Nullable final ClientSession clientSessionFromOperation) {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientDelegate.DelegateOperationExecutor.getClientSession")
                             .startScopedSpan();

            try {
                ClientSession session;
                if (clientSessionFromOperation != null) {
                    isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == originator);
                    TRACER.getCurrentSpan().addAnnotation("Reusing the clientSession from operation");
                    session = clientSessionFromOperation;
                } else {
                    TRACER.getCurrentSpan().addAnnotation("Creating a new client session");
                    session = createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), ReadConcern.DEFAULT,
                            WriteConcern.ACKNOWLEDGED, ReadPreference.primary());
                }
                return session;
            } finally {
                ss.close();
            }
        }
    }
}
