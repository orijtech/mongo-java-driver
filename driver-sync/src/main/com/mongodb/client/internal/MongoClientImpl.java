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
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.Status;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

public final class MongoClientImpl implements MongoClient {

    private final MongoClientSettings settings;
    private final MongoClientDelegate delegate;
    private static final Tracer TRACER = Tracing.getTracer();

    public MongoClientImpl(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(settings, mongoDriverInformation), settings, null);
    }

    public MongoClientImpl(final Cluster cluster, final MongoClientSettings settings,
                           @Nullable final OperationExecutor operationExecutor) {
        this.settings = notNull("settings", settings);
        this.delegate = new MongoClientDelegate(notNull("cluster", cluster),
                Collections.singletonList(settings.getCredential()), this, operationExecutor);
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.getDatabase").startScopedSpan();

        try {
            return new MongoDatabaseImpl(databaseName, settings.getCodecRegistry(), settings.getReadPreference(),
                                         settings.getWriteConcern(), settings.getRetryWrites(), settings.getReadConcern(),
                                         delegate.getOperationExecutor());
        } finally {
            ss.close();
        }
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.listDatabaseNames").startScopedSpan();

        try {
            return createListDatabaseNamesIterable(null);
        } finally {
            ss.close();
        }
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.listDatabaseNames").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createListDatabaseNamesIterable(clientSession);
        } finally {
            ss.close();
        }
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> clazz) {
        return createListDatabasesIterable(null, clazz);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.listDatabases").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createListDatabasesIterable(clientSession, clazz);
        } finally {
            ss.close();
        }
    }

    @Override
    public ClientSession startSession() {
        return startSession(ClientSessionOptions
                .builder()
                .defaultTransactionOptions(TransactionOptions.builder()
                        .readConcern(settings.getReadConcern())
                        .writeConcern(settings.getWriteConcern())
                        .build())
                .build());
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.startSession").startScopedSpan();

        try {
            ClientSession clientSession = delegate.createClientSession(notNull("options", options),
                    settings.getReadConcern(), settings.getWriteConcern(), settings.getReadPreference());
            if (clientSession == null) {
                String msg = "Sessions are not supported by the MongoDB cluster to which this client is connected";
                TRACER.getCurrentSpan().setStatus(Status.INVALID_ARGUMENT.withDescription(msg));
                throw new MongoClientException(msg);
            }
            return clientSession;
        } finally {
            ss.close();
        }
    }

    @Override
    public void close() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.close").startScopedSpan();

        try {
            delegate.close();
        } finally {
            ss.close();
        }
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.watch").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createChangeStreamIterable(clientSession, pipeline, resultClass);
        } finally {
            ss.close();
        }
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.createChangeStreamIterable").startScopedSpan();

        try {
            return new ChangeStreamIterableImpl<TResult>(clientSession, "admin", settings.getCodecRegistry(),
                    settings.getReadPreference(), settings.getReadConcern(), delegate.getOperationExecutor(), pipeline, resultClass,
                    ChangeStreamLevel.CLIENT);
        } finally {
            ss.close();
        }
    }

    public Cluster getCluster() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.getCluster").startScopedSpan();

        try {
            return delegate.getCluster();
        } finally {
            ss.close();
        }
    }

    private static Cluster createCluster(final MongoClientSettings settings,
                                         @Nullable final MongoDriverInformation mongoDriverInformation) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.createCluster").startScopedSpan();

        try {
            notNull("settings", settings);
            List<MongoCredential> credentialList = settings.getCredential() != null ? Collections.singletonList(settings.getCredential())
                    : Collections.<MongoCredential>emptyList();
            return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                    settings.getConnectionPoolSettings(), getStreamFactory(settings, false), getStreamFactory(settings, true),
                    credentialList, getCommandListener(settings.getCommandListeners()), settings.getApplicationName(),
                    mongoDriverInformation, settings.getCompressorList());
        } finally {
            ss.close();
        }
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings, final boolean isHeartbeat) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.getStreamFactory").startScopedSpan();

        try {
            StreamFactoryFactory streamFactoryFactory = settings.getStreamFactoryFactory();
            SocketSettings socketSettings = isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();
            if (streamFactoryFactory == null) {
                return new SocketStreamFactory(socketSettings, settings.getSslSettings());
            } else {
                return streamFactoryFactory.create(socketSettings, settings.getSslSettings());
            }
        } finally {
            ss.close();
        }
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(@Nullable final ClientSession clientSession, final Class<T> clazz) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.createListDatabasesIterable").startScopedSpan();

        try {
            return new ListDatabasesIterableImpl<T>(clientSession, clazz, settings.getCodecRegistry(),
                    ReadPreference.primary(), delegate.getOperationExecutor());
        } finally {
            ss.close();
        }
    }

    private MongoIterable<String> createListDatabaseNamesIterable(final @Nullable ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoClientImpl.createListDatabaseNamesIterable").startScopedSpan();

        try {
            return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
                @Override
                public String apply(final BsonDocument result) {
                    return result.getString("name").getValue();
                }
            });
        } finally {
            ss.close();
        }
    }
}
