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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.session.SessionContext;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

import static org.bson.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ClientSessionBinding implements ReadWriteBinding {
    private final ReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    private static final Tracer TRACER = Tracing.getTracer();

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ReadWriteBinding wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
        this.ownsSession = ownsSession;
        this.session = notNull("session", session);
        this.sessionContext = new SyncClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getReadPreference").startScopedSpan();

        try {
            return wrapped.getReadPreference();
        } finally {
            ss.close();
        }
    }

    @Override
    public int getCount() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getCount").startScopedSpan();

        try {
            return wrapped.getCount();
        } finally {
            ss.close();
        }
    }

    @Override
    public ReadWriteBinding retain() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.retain").startScopedSpan();

        try {
            wrapped.retain();
            return this;
        } finally {
            ss.close();
        }
    }

    @Override
    public void release() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.release").startScopedSpan();

        try {
            wrapped.release();
            closeSessionIfCountIsZero();
        } finally {
            ss.close();
        }
    }

    private void closeSessionIfCountIsZero() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.closeSessionIfCountIsZero").startScopedSpan();

        try {
            if (getCount() == 0 && ownsSession) {
                TRACER.getCurrentSpan().addAnnotation("Closing session since count is zero and ownsSession");
                session.close();
            }
        } finally {
            ss.close();
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        ConnectionSource readConnectionSource = wrapped.getReadConnectionSource();
        return new SessionBindingConnectionSource(readConnectionSource);
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getWriteConnectionSource").startScopedSpan();

        try {
            ConnectionSource writeConnectionSource = wrapped.getWriteConnectionSource();
            return new SessionBindingConnectionSource(writeConnectionSource);
        } finally {
            ss.close();
        }
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getServerDescription").startScopedSpan();

            try {
                return wrapped.getServerDescription();
            } finally {
                ss.close();
            }
        }

        @Override
        public SessionContext getSessionContext() {
            return sessionContext;
        }

        @Override
        public Connection getConnection() {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getConnection").startScopedSpan();

            try {
                return wrapped.getConnection();
            } finally {
                ss.close();
            }
        }

        @Override
        @SuppressWarnings("checkstyle:methodlength")
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.getCount").startScopedSpan();

            try {
                return wrapped.getCount();
            } finally {
                ss.close();
            }
        }

        @Override
        public void release() {
            Scope ss = TRACER.spanBuilder("com.mongodb.client.internal.ClientSessionBinding.release").startScopedSpan();

            try {
                wrapped.release();
                closeSessionIfCountIsZero();
            } finally {
                ss.close();
            }
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext implements SessionContext {

        private final ClientSession clientSession;

        SyncClientSessionContext(final ClientSession clientSession) {
            super(clientSession);
            this.clientSession = clientSession;
        }

        @Override
        public boolean isImplicitSession() {
            return ownsSession;
        }

        @Override
        public boolean notifyMessageSent() {
            return clientSession.notifyMessageSent();
        }

        @Override
        public boolean hasActiveTransaction() {
            return clientSession.hasActiveTransaction();
        }

        @Override
        public ReadConcern getReadConcern() {
            if (clientSession.hasActiveTransaction()) {
                return clientSession.getTransactionOptions().getReadConcern();
            } else {
               return wrapped.getSessionContext().getReadConcern();
            }
        }
    }
}
