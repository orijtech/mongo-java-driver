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

package com.mongodb.client.gridfs;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

final class GridFSBucketImpl implements GridFSBucket {
    private static final int DEFAULT_CHUNKSIZE_BYTES = 255 * 1024;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final boolean disableMD5;
    private volatile boolean checkedIndexes;
    private static final Tracer TRACER = Tracing.getTracer();

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this(notNull("bucketName", bucketName), DEFAULT_CHUNKSIZE_BYTES,
                getFilesCollection(notNull("database", database), bucketName),
                getChunksCollection(database, bucketName), false);
    }

    GridFSBucketImpl(final String bucketName, final int chunkSizeBytes, final MongoCollection<GridFSFile> filesCollection,
                     final MongoCollection<Document> chunksCollection, final boolean disableMD5) {
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = chunkSizeBytes;
        this.filesCollection = notNull("filesCollection", filesCollection);
        this.chunksCollection = notNull("chunksCollection", chunksCollection);
        this.disableMD5 = disableMD5;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    @Override
    public ReadPreference getReadPreference() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getReadPreference").startScopedSpan();

        try {
            return filesCollection.getReadPreference();
        } finally {
            ss.close();
        }
    }

    @Override
    public WriteConcern getWriteConcern() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getWriteConcern").startScopedSpan();

        try {
            return filesCollection.getWriteConcern();
        } finally {
            ss.close();
        }
    }

    @Override
    public ReadConcern getReadConcern() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getReadConcern").startScopedSpan();

        try {
            return filesCollection.getReadConcern();
        } finally {
            ss.close();
        }
    }

    @Override
    public boolean getDisableMD5() {
        return disableMD5;
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection, disableMD5);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadPreference(readPreference),
                chunksCollection.withReadPreference(readPreference), disableMD5);
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withWriteConcern(writeConcern),
                chunksCollection.withWriteConcern(writeConcern), disableMD5);
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection.withReadConcern(readConcern),
                chunksCollection.withReadConcern(readConcern), disableMD5);
    }

    @Override
    public GridFSBucket withDisableMD5(final boolean disableMD5) {
        return new GridFSBucketImpl(bucketName, chunkSizeBytes, filesCollection, chunksCollection, disableMD5);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(new BsonObjectId(), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        return openUploadStream(new BsonObjectId(), filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        return createGridFSUploadStream(null, id, filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename,
                                               final GridFSUploadOptions options) {
        return openUploadStream(clientSession, new BsonObjectId(), filename, options);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final ObjectId id, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(id), filename);
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename) {
        return openUploadStream(clientSession, id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                               final GridFSUploadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.openUploadStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createGridFSUploadStream(clientSession, id, filename, options);
        } finally {
            ss.close();
        }
    }

    private GridFSUploadStream createGridFSUploadStream(@Nullable final ClientSession clientSession, final BsonValue id,
                                                        final String filename, final GridFSUploadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.createGridFSUploadStream").startScopedSpan();

        try {
            notNull("options", options);
            Integer chunkSizeBytes = options.getChunkSizeBytes();
            int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
            checkCreateIndex(clientSession);
            return new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize,
                    disableMD5, options.getMetadata());
        } finally {
            ss.close();
        }
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source) {
        return uploadFromStream(filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source, final GridFSUploadOptions options) {
        ObjectId id = new ObjectId();
        uploadFromStream(new BsonObjectId(id), filename, source, options);
        return id;
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source,
                                 final GridFSUploadOptions options) {
        executeUploadFromStream(null, id, filename, source, options);
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source) {
        return uploadFromStream(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source,
                                     final GridFSUploadOptions options) {
        ObjectId id = new ObjectId();
        uploadFromStream(clientSession, new BsonObjectId(id), filename, source, options);
        return id;
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source,
                                 final GridFSUploadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.uploadFromStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            executeUploadFromStream(clientSession, id, filename, source, options);
        } finally {
            ss.close();
        }
    }

    private void executeUploadFromStream(@Nullable final ClientSession clientSession, final BsonValue id, final String filename,
                                         final InputStream source, final GridFSUploadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.uploadFromStream").startScopedSpan();

        GridFSUploadStream uploadStream = createGridFSUploadStream(clientSession, id, filename, options);
        Integer chunkSizeBytes = options.getChunkSizeBytes();
        int chunkSize = chunkSizeBytes == null ? this.chunkSizeBytes : chunkSizeBytes;
        byte[] buffer = new byte[chunkSize];
        TRACER.getCurrentSpan().putAttribute("chunkSize", AttributeValue.longAttributeValue(chunkSize));
        int len;
        try {
            while ((len = source.read(buffer)) != -1) {
                TRACER.getCurrentSpan().addAnnotation("writingStream");
                TRACER.getCurrentSpan().putAttribute("byte_len", AttributeValue.longAttributeValue(len));
                uploadStream.write(buffer, 0, len);
            }
            TRACER.getCurrentSpan().addAnnotation("Closing the uploadStream");
            uploadStream.close();
        } catch (IOException e) {
            uploadStream.abort();
            TRACER.getCurrentSpan().addAnnotation("Encountered an IOException");
            TRACER.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.toString()));
            throw new MongoGridFSException("IOException when reading from the InputStream", e);
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return openDownloadStream(new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        return createGridFSDownloadStream(null, getFileInfoById(null, id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        return createGridFSDownloadStream(null, getFileByName(null, filename, options));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        return openDownloadStream(clientSession, new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.openDownloadStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createGridFSDownloadStream(clientSession, getFileInfoById(clientSession, id));
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.openDownloadStream").startScopedSpan();

        try {
            return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename,
                                                   final GridFSDownloadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.openDownloadStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createGridFSDownloadStream(clientSession, getFileByName(clientSession, filename, options));
        } finally {
            ss.close();
        }
    }

    private GridFSDownloadStream createGridFSDownloadStream(@Nullable final ClientSession clientSession, final GridFSFile gridFSFile) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.createGridFSDownloadStream").startScopedSpan();

        try {
            return new GridFSDownloadStreamImpl(clientSession, gridFSFile, chunksCollection);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final ObjectId id, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(new BsonObjectId(id), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final BsonValue id, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(openDownloadStream(id), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(filename, destination, new GridFSDownloadOptions());
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination, final GridFSDownloadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(openDownloadStream(filename, options), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final ObjectId id, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(clientSession, new BsonObjectId(id), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final BsonValue id, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            downloadToStream(openDownloadStream(clientSession, id), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions());
        } finally {
            ss.close();
        }
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination,
                                 final GridFSDownloadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            downloadToStream(openDownloadStream(clientSession, filename, options), destination);
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSFindIterable find() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.find").startScopedSpan();

        try {
            return createGridFSFindIterable(null, null);
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSFindIterable find(final Bson filter) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.find").startScopedSpan();

        try {
            notNull("filter", filter);
            return createGridFSFindIterable(null, filter);
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.find").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            return createGridFSFindIterable(clientSession, null);
        } finally {
            ss.close();
        }
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession, final Bson filter) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.find").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            notNull("filter", filter);
            return createGridFSFindIterable(clientSession, filter);
        } finally {
            ss.close();
        }
    }

    private GridFSFindIterable createGridFSFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.createGridFSFindIterable").startScopedSpan();

        try {
            return new GridFSFindIterableImpl(createFindIterable(clientSession, filter));
        } finally {
            ss.close();
        }
    }

    @Override
    public void delete(final ObjectId id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.delete").startScopedSpan();

        try {
            delete(new BsonObjectId(id));
        } finally {
            ss.close();
        }
    }

    @Override
    public void delete(final BsonValue id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.delete").startScopedSpan();

        try {
            executeDelete(null, id);
        } finally {
            ss.close();
        }
    }

    @Override
    public void delete(final ClientSession clientSession, final ObjectId id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.delete").startScopedSpan();

        try {
            delete(clientSession, new BsonObjectId(id));
        } finally {
            ss.close();
        }
    }

    @Override
    public void delete(final ClientSession clientSession, final BsonValue id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.delete").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            executeDelete(clientSession, id);
        } finally {
            ss.close();
        }
    }

    private void executeDelete(@Nullable final ClientSession clientSession, final BsonValue id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.executeDelete").startScopedSpan();

        try {
            DeleteResult result;
            if (clientSession != null) {
                TRACER.getCurrentSpan().addAnnotation("Client session is non-nil");
                result = filesCollection.deleteOne(clientSession, new BsonDocument("_id", id));
                chunksCollection.deleteMany(clientSession, new BsonDocument("files_id", id));
            } else {
                TRACER.getCurrentSpan().addAnnotation("Client session is nil");
                result = filesCollection.deleteOne(new BsonDocument("_id", id));
                chunksCollection.deleteMany(new BsonDocument("files_id", id));
            }

            if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
                TRACER.getCurrentSpan().addAnnotation("No file found");
                String msg = format("No file found with the id: %s", id);
                TRACER.getCurrentSpan().setStatus(Status.NOT_FOUND.withDescription(msg));
                throw new MongoGridFSException(msg);
            }
        } finally {
            ss.close();
        }
    }

    @Override
    public void rename(final ObjectId id, final String newFilename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.rename").startScopedSpan();

        try {
            rename(new BsonObjectId(id), newFilename);
        } finally {
            ss.close();
        }
    }

    @Override
    public void rename(final BsonValue id, final String newFilename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.rename").startScopedSpan();

        try {
            executeRename(null, id, newFilename);
        } finally {
            ss.close();
        }
    }

    @Override
    public void rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.rename").startScopedSpan();

        try {
            rename(clientSession, new BsonObjectId(id), newFilename);
        } finally {
            ss.close();
        }
    }

    @Override
    public void rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.rename").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            executeRename(clientSession, id, newFilename);
        } finally {
            ss.close();
        }
    }

    private void executeRename(@Nullable final ClientSession clientSession, final BsonValue id, final String newFilename) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.executeRename").startScopedSpan();

        try {
            UpdateResult updateResult;
            if (clientSession != null) {
                updateResult = filesCollection.updateOne(clientSession, new BsonDocument("_id", id),
                        new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
            } else {
                updateResult = filesCollection.updateOne(new BsonDocument("_id", id),
                        new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
            }

            if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {
                String msg = format("No file found with the id: %s", id);
                TRACER.getCurrentSpan().setStatus(Status.NOT_FOUND.withDescription(msg));
                throw new MongoGridFSException(msg);
            }
        } finally {
            ss.close();
        }
    }

    @Override
    public void drop() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.drop").startScopedSpan();

        try {
            filesCollection.drop();
            chunksCollection.drop();
        } finally {
            ss.close();
        }
    }

    @Override
    public void drop(final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.drop").startScopedSpan();

        try {
            notNull("clientSession", clientSession);
            filesCollection.drop(clientSession);
            chunksCollection.drop(clientSession);
        } finally {
            ss.close();
        }
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public GridFSDownloadStream openDownloadStreamByName(final String filename) {
        return openDownloadStreamByName(filename, new com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions());
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public GridFSDownloadStream openDownloadStreamByName(final String filename,
                                                         final com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions options) {
        return openDownloadStream(filename, new GridFSDownloadOptions().revision(options.getRevision()));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void downloadToStreamByName(final String filename, final OutputStream destination) {
        downloadToStreamByName(filename, destination, new com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions());
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public void downloadToStreamByName(final String filename, final OutputStream destination,
                                       final com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions options) {
        downloadToStream(filename, destination, new GridFSDownloadOptions().revision(options.getRevision()));
    }

    private static MongoCollection<GridFSFile> getFilesCollection(final MongoDatabase database, final String bucketName) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getFilesCollection").startScopedSpan();

        try {
            return database.getCollection(bucketName + ".files", GridFSFile.class).withCodecRegistry(
                    fromRegistries(database.getCodecRegistry(), MongoClientSettings.getDefaultCodecRegistry())
            );
        } finally {
            ss.close();
        }
    }

    private static MongoCollection<Document> getChunksCollection(final MongoDatabase database, final String bucketName) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getChunksCollection").startScopedSpan();

        try {
            return database.getCollection(bucketName + ".chunks").withCodecRegistry(MongoClientSettings.getDefaultCodecRegistry());
        } finally {
            ss.close();
        }
    }

    private void checkCreateIndex(@Nullable final ClientSession clientSession) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.checkCreateIndex").startScopedSpan();

        try {
            TRACER.getCurrentSpan().putAttribute("checkedIndexes", AttributeValue.booleanAttributeValue(checkedIndexes));

            if (!checkedIndexes) {
                TRACER.getCurrentSpan().addAnnotation("Unchecked indexes");
                if (collectionIsEmpty(clientSession, filesCollection.withDocumentClass(Document.class).withReadPreference(primary()))) {
                    TRACER.getCurrentSpan().addAnnotation("Collection is empty");
                    Document filesIndex = new Document("filename", 1).append("uploadDate", 1);
                    if (!hasIndex(clientSession, filesCollection.withReadPreference(primary()), filesIndex)) {
                        TRACER.getCurrentSpan().addAnnotation("Creating the filesIndex");
                        createIndex(clientSession, filesCollection, filesIndex, new IndexOptions());
                    }
                    Document chunksIndex = new Document("files_id", 1).append("n", 1);
                    if (!hasIndex(clientSession, chunksCollection.withReadPreference(primary()), chunksIndex)) {
                        TRACER.getCurrentSpan().addAnnotation("Creating the chunksIndex");
                        createIndex(clientSession, chunksCollection, chunksIndex, new IndexOptions().unique(true));
                    }
                }
                checkedIndexes = true;
            }
        } finally {
            ss.close();
        }
    }

    private <T> boolean collectionIsEmpty(@Nullable final ClientSession clientSession, final MongoCollection<T> collection) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.collectionIsEmpty").startScopedSpan();

        try {
            if (clientSession != null) {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is non-null");
                return collection.find(clientSession).projection(new Document("_id", 1)).first() == null;
            } else {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is null so creating a projection then checking it");
                return collection.find().projection(new Document("_id", 1)).first() == null;
            }
        } finally {
            ss.close();
        }
    }

    private <T> boolean hasIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection, final Document index) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.hasIndex").startScopedSpan();

        try {
            boolean hasIndex = false;
            ListIndexesIterable<Document> listIndexesIterable;
            if (clientSession != null) {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is non-null");
                listIndexesIterable = collection.listIndexes(clientSession);
            } else {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is null");
                listIndexesIterable = collection.listIndexes();
            }

            ArrayList<Document> indexes = listIndexesIterable.into(new ArrayList<Document>());
            for (Document indexDoc : indexes) {
                if (indexDoc.get("key", Document.class).equals(index)) {
                    hasIndex = true;
                    break;
                }
            }
            TRACER.getCurrentSpan().putAttribute("hasIndex", AttributeValue.booleanAttributeValue(hasIndex));
            return hasIndex;
        } finally {
            ss.close();
        }
    }

    private <T> void createIndex(@Nullable final ClientSession clientSession, final MongoCollection<T> collection, final Document index,
                                 final IndexOptions indexOptions) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.createIndex").startScopedSpan();

        try {
            if (clientSession != null) {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is non-null");
                collection.createIndex(clientSession, index, indexOptions);
            } else {
                TRACER.getCurrentSpan().addAnnotation("ClientSession is null");
                collection.createIndex(index, indexOptions);
            }
        } finally {
            ss.close();
        }
    }

    private GridFSFile getFileByName(@Nullable final ClientSession clientSession, final String filename,
                                     final GridFSDownloadOptions options) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getFileByName").startScopedSpan();

        try {
            int revision = options.getRevision();
            int skip;
            int sort;
            if (revision >= 0) {
                skip = revision;
                sort = 1;
            } else {
                skip = (-revision) - 1;
                sort = -1;
            }

            TRACER.getCurrentSpan().putAttribute("revision", AttributeValue.longAttributeValue(revision));
            TRACER.getCurrentSpan().putAttribute("sort", AttributeValue.longAttributeValue(sort));
            TRACER.getCurrentSpan().putAttribute("skip", AttributeValue.longAttributeValue(skip));

            GridFSFile fileInfo = createGridFSFindIterable(clientSession, new Document("filename", filename)).skip(skip)
                    .sort(new Document("uploadDate", sort)).first();
            if (fileInfo == null) {
                String msg = format("No file found with the filename: %s and revision: %s", filename, revision);
                TRACER.getCurrentSpan().setStatus(Status.NOT_FOUND.withDescription(msg));
                throw new MongoGridFSException(msg);
            }
            return fileInfo;
        } finally {
            ss.close();
        }
    }

    private GridFSFile getFileInfoById(@Nullable final ClientSession clientSession, final BsonValue id) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.getFileInfoById").startScopedSpan();

        try {
            notNull("id", id);
            GridFSFile fileInfo = createFindIterable(clientSession, new Document("_id", id)).first();
            if (fileInfo == null) {
                String msg = format("No file found with the id: %s", id);
                TRACER.getCurrentSpan().setStatus(Status.NOT_FOUND.withDescription(msg));
                throw new MongoGridFSException(msg);
            }
            return fileInfo;
        } finally {
            ss.close();
        }
    }

    private FindIterable<GridFSFile> createFindIterable(@Nullable final ClientSession clientSession, @Nullable final Bson filter) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.createFindIterable").startScopedSpan();

        try {
            FindIterable<GridFSFile> findIterable;
            if (clientSession != null) {
                findIterable = filesCollection.find(clientSession);
            } else {
                findIterable = filesCollection.find();
            }
            if (filter != null) {
                findIterable = findIterable.filter(filter);
            }
            return findIterable;
        } finally {
            ss.close();
        }
    }

    private void downloadToStream(final GridFSDownloadStream downloadStream, final OutputStream destination) {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.gridfs.GridFSBucketImpl.downloadToStream").startScopedSpan();

        byte[] buffer = new byte[downloadStream.getGridFSFile().getChunkSize()];
        int len;
        MongoGridFSException savedThrowable = null;
        try {
            while ((len = downloadStream.read(buffer)) != -1) {
                destination.write(buffer, 0, len);
            }
        } catch (IOException e) {
            savedThrowable = new MongoGridFSException("IOException when reading from the OutputStream", e);
        } catch (Exception e) {
            savedThrowable = new MongoGridFSException("Unexpected Exception when reading GridFS and writing to the Stream", e);
        } finally {
            try {
                downloadStream.close();
            } catch (Exception e) {
                // Do nothing
            }

            try {
                if (savedThrowable != null) {
                    TRACER.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(savedThrowable.toString()));
                    throw savedThrowable;
                }
            } finally {
                ss.close();
            }
        }
    }
}
