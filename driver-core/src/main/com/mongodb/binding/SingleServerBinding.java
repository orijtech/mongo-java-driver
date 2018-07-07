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

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.selector.ServerAddressSelector;
import com.mongodb.session.SessionContext;

import static com.mongodb.assertions.Assertions.notNull;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * @since 3.0
 */
public class SingleServerBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;
    private final ReadPreference readPreference;
    private static final Tracer TRACER = Tracing.getTracer();

    /**
     * Creates an instance, defaulting to {@link com.mongodb.ReadPreference#primary()} for reads.
     * @param cluster       a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress a non-null  address of the server to bind to
     */
    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress) {
        this(cluster, serverAddress, ReadPreference.primary());
    }

    /**
     * Creates an instance.
     * @param cluster        a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress  a non-null  address of the server to bind to
     * @param readPreference a non-null  ReadPreference for read operations
     */
    public SingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final ReadPreference readPreference) {
        this.cluster = notNull("cluster", cluster);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.readPreference = notNull("readPreference", readPreference);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.getWriteConnectionSource").startScopedSpan();

        try {
            return new SingleServerBindingConnectionSource();
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
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.getReadConnectionSource").startScopedSpan();

        try {
            return new SingleServerBindingConnectionSource();
        } finally {
            ss.close();
        }
    }

    @Override
    public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
    }

    @Override
    public SingleServerBinding retain() {
        Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.retain").startScopedSpan();

        try {
            super.retain();
            return this;
        } finally {
            ss.close();
        }
    }

    private final class SingleServerBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;

        private SingleServerBindingConnectionSource() {
            SingleServerBinding.this.retain();
            server = cluster.selectServer(new ServerAddressSelector(serverAddress));
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return NoOpSessionContext.INSTANCE;
        }

        @Override
        public Connection getConnection() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.SingleServerBindingConnection.getConnection")
                             .startScopedSpan();

            try {
                return cluster.selectServer(new ServerAddressSelector(serverAddress)).getConnection();
            } finally {
                ss.close();
            }
        }

        @Override
        public ConnectionSource retain() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.SingleServerBindingConnection.retain")
                             .startScopedSpan();

            try {
                super.retain();
                return this;
            } finally {
                ss.close();
            }
        }

        @Override
        public void release() {
            Scope ss = TRACER.spanBuilder("com.mongodb.binding.SingleServerBinding.SingleServerBindingConnection.release")
                             .startScopedSpan();

            try {
                super.release();
                if (super.getCount() == 0) {
                    SingleServerBinding.this.release();
                }
            } finally {
                ss.close();
            }
        }
    }
}

