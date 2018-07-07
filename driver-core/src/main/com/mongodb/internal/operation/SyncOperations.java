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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.internal.client.model.CountStrategy;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.MapReduceBatchCursor;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;


import java.util.List;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class SyncOperations<TDocument> {
    private final Operations<TDocument> operations;
    private static final Tracer TRACER = Tracing.getTracer();

    public SyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry) {
        this(null, documentClass, readPreference, codecRegistry, WriteConcern.ACKNOWLEDGED, false);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry) {
        this(namespace, documentClass, readPreference, codecRegistry, WriteConcern.ACKNOWLEDGED, false);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final WriteConcern writeConcern, final boolean retryWrites) {
        this.operations = new Operations<TDocument>(namespace, documentClass, readPreference, codecRegistry, writeConcern, retryWrites);
    }

    public ReadOperation<Long> count(final Bson filter, final CountOptions options, final CountStrategy countStrategy) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.count").startScopedSpan();

        try {
            return operations.count(filter, options, countStrategy);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> findFirst(final Bson filter, final Class<TResult> resultClass,
                                                                   final FindOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.findFirst").startScopedSpan();

        try {
            return operations.findFirst(filter, resultClass, options);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> find(final Bson filter, final Class<TResult> resultClass,
                                                              final FindOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.find").startScopedSpan();

        try {
            return operations.find(filter, resultClass, options);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> find(final MongoNamespace findNamespace, final Bson filter,
                                                              final Class<TResult> resultClass, final FindOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.find").startScopedSpan();

        try {
            return operations.find(findNamespace, filter, resultClass, options);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> distinct(final String fieldName, final Bson filter,
                                                                  final Class<TResult> resultClass, final long maxTimeMS,
                                                                  final Collation collation) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.distinct").startScopedSpan();

        try {
            return operations.distinct(fieldName, filter, resultClass, maxTimeMS, collation);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                                                   final long maxTimeMS, final long maxAwaitTimeMS, final Integer batchSize,
                                                                   final Collation collation, final Bson hint, final String comment,
                                                                   final Boolean allowDiskUse, final Boolean useCursor) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.aggregate").startScopedSpan();

        try {
            return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, batchSize, collation, hint, comment, allowDiskUse,
                    useCursor);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<Void> aggregateToCollection(final List<? extends Bson> pipeline, final long maxTimeMS,
                                                      final Boolean allowDiskUse, final Boolean bypassDocumentValidation,
                                                      final Collation collation, final Bson hint, final String comment) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.aggregateToCollection").startScopedSpan();

        try {
            return operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint, comment);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<MapReduceStatistics> mapReduceToCollection(final String databaseName, final String collectionName,
                                                                     final String mapFunction, final String reduceFunction,
                                                                     final String finalizeFunction, final Bson filter, final int limit,
                                                                     final long maxTimeMS, final boolean jsMode, final Bson scope,
                                                                     final Bson sort, final boolean verbose, final MapReduceAction action,
                                                                     final boolean nonAtomic, final boolean sharded,
                                                                     final Boolean bypassDocumentValidation, final Collation collation) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.mapReduceToCollection").startScopedSpan();

        try {
            return operations.mapReduceToCollection(databaseName, collectionName, mapFunction, reduceFunction, finalizeFunction, filter,
                    limit, maxTimeMS, jsMode, scope, sort, verbose, action, nonAtomic, sharded, bypassDocumentValidation, collation);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<MapReduceBatchCursor<TResult>> mapReduce(final String mapFunction, final String reduceFunction,
                                                                            final String finalizeFunction, final Class<TResult> resultClass,
                                                                            final Bson filter, final int limit,
                                                                            final long maxTimeMS, final boolean jsMode, final Bson scope,
                                                                            final Bson sort, final boolean verbose,
                                                                            final Collation collation) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.mapReduce").startScopedSpan();

        try {
            return operations.mapReduce(mapFunction, reduceFunction, finalizeFunction, resultClass, filter, limit, maxTimeMS, jsMode, scope,
                    sort, verbose, collation);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<TDocument> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.findOneAndDelete").startScopedSpan();

        try {
            return operations.findOneAndDelete(filter, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement,
                                                       final FindOneAndReplaceOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.findOneAndReplace").startScopedSpan();

        try {
            return operations.findOneAndReplace(filter, replacement, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<TDocument> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.findOneAndUpdate").startScopedSpan();

        try {
            return operations.findOneAndUpdate(filter, update, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> insertOne(final TDocument document, final InsertOneOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.insertOne").startScopedSpan();

        try {
            return operations.insertOne(document, options);
        } finally {
            ss.close();
        }
    }


    public WriteOperation<BulkWriteResult> replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.replaceOne").startScopedSpan();

        try {
            return operations.replaceOne(filter, replacement, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.deleteOne").startScopedSpan();

        try {
            return operations.deleteOne(filter, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.deleteMany").startScopedSpan();

        try {
            return operations.deleteMany(filter, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.updateOne").startScopedSpan();

        try {
            return operations.updateOne(filter, update, updateOptions);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.updateMany").startScopedSpan();

        try {
            return operations.updateMany(filter, update, updateOptions);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<BulkWriteResult> insertMany(final List<? extends TDocument> documents,
                                                      final InsertManyOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.insertMany").startScopedSpan();

        try {
            return operations.insertMany(documents, options);
        } finally {
            ss.close();
        }
    }

    @SuppressWarnings("unchecked")
    public WriteOperation<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                                                     final BulkWriteOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.bulkWrite").startScopedSpan();

        try {
            return operations.bulkWrite(requests, options);
        } finally {
            ss.close();
        }
    }


    public WriteOperation<Void> dropCollection() {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.dropCollection").startScopedSpan();

        try {
            return operations.dropCollection();
        } finally {
            ss.close();
        }
    }

    public WriteOperation<Void> renameCollection(final MongoNamespace newCollectionNamespace,
                                                 final RenameCollectionOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.renameCollection").startScopedSpan();

        try {
            return operations.renameCollection(newCollectionNamespace, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<Void> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.createIndexes").startScopedSpan();

        try {
            return operations.createIndexes(indexes, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<Void> dropIndex(final String indexName, final DropIndexOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.dropIndex").startScopedSpan();

        try {
            return operations.dropIndex(indexName, options);
        } finally {
            ss.close();
        }
    }

    public WriteOperation<Void> dropIndex(final Bson keys, final DropIndexOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.dropIndex").startScopedSpan();

        try {
            return operations.dropIndex(keys, options);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listCollections(final String databaseName, final Class<TResult> resultClass,
                                                                         final Bson filter, final boolean collectionNamesOnly,
                                                                         final Integer batchSize, final long maxTimeMS) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.listCollections").startScopedSpan();

        try {
            return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, batchSize, maxTimeMS);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listDatabases(final Class<TResult> resultClass, final Bson filter,
                                                                       final Boolean nameOnly, final long maxTimeMS) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.listDatabases").startScopedSpan();

        try {
            return operations.listDatabases(resultClass, filter, nameOnly, maxTimeMS);
        } finally {
            ss.close();
        }
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listIndexes(final Class<TResult> resultClass, final Integer batchSize,
                                                                     final long maxTimeMS) {
        Scope ss = TRACER.spanBuilder("com.mongodb.internal.operation.SyncOperations.listIndexes").startScopedSpan();

        try {
            return operations.listIndexes(resultClass, batchSize, maxTimeMS);
        } finally {
            ss.close();
        }
    }
}
