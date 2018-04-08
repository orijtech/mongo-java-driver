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

package com.mongodb.binding;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.selector.WritableServerSelector;
import com.mongodb.session.SessionContext;

import static com.mongodb.assertions.Assertions.notNull;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * @since 3.0
 */
public class ClusterBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private static final Tracer TRACER = Tracing.getTracer();

    /**
     * Creates an instance.
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     * @deprecated Prefer {@link #ClusterBinding(Cluster, ReadPreference, ReadConcern)}
     */
    @Deprecated
    public ClusterBinding(final Cluster cluster, final ReadPreference readPreference) {
        this(cluster, readPreference, ReadConcern.DEFAULT);
    }

    /**
     * Creates an instance.
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     * @param readConcern    a non-null read concern
     * @since 3.8
     */
    public ClusterBinding(final Cluster cluster, final ReadPreference readPreference, final ReadConcern readConcern) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
    }

    @Override
    public ReadWriteBinding retain() {
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.retain").startScopedSpan();

        try {
            super.retain();
            return this;
        } finally {
            ss.close();
        }
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.getReadConnectionSource").startScopedSpan();

        try {
            return new ClusterBindingConnectionSource(new ReadPreferenceServerSelector(readPreference));
        } finally {
            ss.close();
        }
    }

    @Override
    public SessionContext getSessionContext() {
        return new ReadConcernAwareNoOpSessionContext(readConcern);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.getWriteConnectionSource").startScopedSpan();

        try {
            return new ClusterBindingConnectionSource(new WritableServerSelector());
        } finally {
            ss.close();
        }
    }

    private final class ClusterBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;

        private ClusterBindingConnectionSource(final ServerSelector serverSelector) {
            this.server = cluster.selectServer(serverSelector);
            ClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.ClusterBindingConnection.getServerDescription")
                             .startScopedSpan();

            try {
                return server.getDescription();
            } finally {
                ss.close();
            }
        }

        @Override
        public SessionContext getSessionContext() {
            return new ReadConcernAwareNoOpSessionContext(readConcern);
        }

        @Override
        public Connection getConnection() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.ClusterBindingConnectionSource.getConnection")
                             .startScopedSpan();

            try {
                return server.getConnection();
            } finally {
                ss.close();
            }
        }

        public ConnectionSource retain() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.ClusterBindingConnectionSource.retain")
                             .startScopedSpan();

            try {
                super.retain();
                ClusterBinding.this.retain();
                return this;
            } finally {
                ss.close();
            }
        }

        @Override
        public void release() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.ClusterBinding.ClusterBindingConnectionSource.release")
                             .startScopedSpan();

            try {
                super.release();
                ClusterBinding.this.release();
            } finally {
                ss.close();
            }
        }
    }
}
