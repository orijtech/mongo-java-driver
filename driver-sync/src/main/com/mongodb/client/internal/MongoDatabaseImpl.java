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

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateViewOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.client.ClientSession;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.Tracer;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;

    private static final Tracer TRACER = Tracing.getTracer();

    public MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                             final WriteConcern writeConcern, final boolean retryWrites, final ReadConcern readConcern,
                             final OperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new MongoCollectionImpl<TDocument>(new MongoNamespace(name, collectionName), documentClass, codecRegistry, readPreference,
                writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public Document runCommand(final Bson command) {
        return runCommand(command, Document.class);
    }

    @Override
    public Document runCommand(final Bson command, final ReadPreference readPreference) {
        return runCommand(command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final Class<TResult> resultClass) {
        return runCommand(command, ReadPreference.primary(), resultClass);
    }

    @Override
    public <TResult> TResult runCommand(final Bson command, final ReadPreference readPreference, final Class<TResult> resultClass) {
        return executeCommand(null, command, readPreference, resultClass);
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command) {
        return runCommand(clientSession, command, ReadPreference.primary(), Document.class);
    }

    @Override
    public Document runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference) {
        return runCommand(clientSession, command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final Class<TResult> resultClass) {
        return runCommand(clientSession, command, ReadPreference.primary(), resultClass);
    }

    @Override
    public <TResult> TResult runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                        final Class<TResult> resultClass) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.runCommand").startScopedSpan();
        TRACER.getCurrentSpan().addAnnotation("RunCommand with specified ReadPreference");

        try {
            notNull("clientSession", clientSession);
            return executeCommand(clientSession, command, readPreference, resultClass);
        } finally {
            ss.close();
        }
    }

    private <TResult> TResult executeCommand(@Nullable final ClientSession clientSession, final Bson command,
                                             final ReadPreference readPreference, final Class<TResult> resultClass) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.executeCommand").startScopedSpan();

        try {
            notNull("readPreference", readPreference);
            if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
                throw new MongoClientException("Read preference in a transaction must be primary");
            }
            return executor.execute(new CommandReadOperation<TResult>(getName(), toBsonDocument(command), codecRegistry.get(resultClass)),
                    readPreference, readConcern, clientSession);
        } finally {
            ss.close();
        }
    }

    @Override
    public void drop() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.drop").startScopedSpan();

        try {
            executeDrop(null);
        } finally {
            ss.close();
        }
    }

    @Override
    public void drop(final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.drop").startScopedSpan();
        TRACER.getCurrentSpan().addAnnotation("Dropping Database with ClientSession");

        try {
            notNull("clientSession", clientSession);
            executeDrop(clientSession);
        } finally {
            ss.close();
        }
    }

    private void executeDrop(@Nullable final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.executeDrop").startScopedSpan();
        TRACER.getCurrentSpan().addAnnotation("Dropping database");

        try {
            executor.execute(new DropDatabaseOperation(name, getWriteConcern()), readConcern, clientSession);
        } finally {
            ss.close();
        }
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return createListCollectionNamesIterable(null);
    }

    @Override
    public MongoIterable<String> listCollectionNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListCollectionNamesIterable(clientSession);
    }

    private MongoIterable<String> createListCollectionNamesIterable(@Nullable final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.createListCollectionnamesIterable").startScopedSpan();
        TRACER.getCurrentSpan().putAttribute("null client session", AttributeValue.booleanAttributeValue(clientSession == null));

        try {
            return createListCollectionsIterable(clientSession, BsonDocument.class, true)
                    .map(new Function<BsonDocument, String>() {
                        @Override
                        public String apply(final BsonDocument result) {
                            return result.getString("name").getValue();
                        }
                    });
        } finally {
            ss.close();
        }
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final Class<TResult> resultClass) {
        return createListCollectionsIterable(null, resultClass, false);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListCollectionsIterable(clientSession, resultClass, false);
    }

    private <TResult> ListCollectionsIterable<TResult> createListCollectionsIterable(@Nullable final ClientSession clientSession,
                                                                                     final Class<TResult> resultClass,
                                                                                     final boolean collectionNamesOnly) {
        return new ListCollectionsIterableImpl<TResult>(clientSession, name, collectionNamesOnly, resultClass, codecRegistry,
                ReadPreference.primary(), executor);
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        executeCreateCollection(null, collectionName, createCollectionOptions);
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName) {
        createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName,
                                 final CreateCollectionOptions createCollectionOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.createCollection").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            executeCreateCollection(clientSession, collectionName, createCollectionOptions);
        } finally {
            ss.close();
        }
    }

    @SuppressWarnings("deprecation")
    private void executeCreateCollection(@Nullable final ClientSession clientSession, final String collectionName,
                                         final CreateCollectionOptions createCollectionOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.executeCreateCollection").startScopedSpan();

        try {
            CreateCollectionOperation operation = new CreateCollectionOperation(name, collectionName, writeConcern)
                    .collation(createCollectionOptions.getCollation())
                    .capped(createCollectionOptions.isCapped())
                    .sizeInBytes(createCollectionOptions.getSizeInBytes())
                    .autoIndex(createCollectionOptions.isAutoIndex())
                    .maxDocuments(createCollectionOptions.getMaxDocuments())
                    .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes())
                    .storageEngineOptions(toBsonDocument(createCollectionOptions.getStorageEngineOptions()));

            IndexOptionDefaults indexOptionDefaults = createCollectionOptions.getIndexOptionDefaults();
            Bson storageEngine = indexOptionDefaults.getStorageEngine();
            if (storageEngine != null) {
                operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine)));
            }
            ValidationOptions validationOptions = createCollectionOptions.getValidationOptions();
            Bson validator = validationOptions.getValidator();
            if (validator != null) {
                operation.validator(toBsonDocument(validator));
            }
            if (validationOptions.getValidationLevel() != null) {
                operation.validationLevel(validationOptions.getValidationLevel());
            }
            if (validationOptions.getValidationAction() != null) {
                operation.validationAction(validationOptions.getValidationAction());
            }
            executor.execute(operation, readConcern, clientSession);
        } finally {
            ss.close();
        }
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final CreateViewOptions createViewOptions) {
        executeCreateView(null, viewName, viewOn, pipeline, createViewOptions);
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline) {
        createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.createView").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            executeCreateView(clientSession, viewName, viewOn, pipeline, createViewOptions);
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
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.watch").startScopedSpan();

        try {
            return createChangeStreamIterable(null, pipeline, resultClass);
        } finally {
            ss.close();
        }
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
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.watch").startScopedSpan();

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
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.createChangeStreamIterable").startScopedSpan();

        try {
            return new ChangeStreamIterableImpl<TResult>(clientSession, name, codecRegistry, readPreference,
                    readConcern, executor, pipeline, resultClass, ChangeStreamLevel.DATABASE);
        } finally {
            ss.close();
        }
    }

    private void executeCreateView(@Nullable final ClientSession clientSession, final String viewName, final String viewOn,
                                   final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.executeCreateView").startScopedSpan();

        try {
            notNull("createViewOptions", createViewOptions);
            executor.execute(new CreateViewOperation(name, viewName, viewOn, createBsonDocumentList(pipeline), writeConcern)
                            .collation(createViewOptions.getCollation()),
                    readConcern, clientSession);
        } finally {
            ss.close();
        }
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.MongoDatabaseImpl.createBsonDocumentList").startScopedSpan();

        try {
            notNull("pipeline", pipeline);
            TRACER.getCurrentSpan().putAttribute("pipeline length", AttributeValue.longAttributeValue(pipeline.size()));
            List<BsonDocument> bsonDocumentPipeline = new ArrayList<BsonDocument>(pipeline.size());
            for (Bson obj : pipeline) {
                if (obj == null) {
                    throw new IllegalArgumentException("pipeline can not contain a null value");
                }
                bsonDocumentPipeline.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
            }
            return bsonDocumentPipeline;
        } finally {
            ss.close();
        }
    }

    @Nullable
    private BsonDocument toBsonDocument(@Nullable final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}
