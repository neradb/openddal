/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.openddal.engine.Session;
import com.openddal.engine.spi.Transaction;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.repo.ConnectionProvider;
import com.openddal.repo.tx.ConnectionHolder.Callback;
import com.openddal.util.New;

/**
 * @author jorgie.li
 *
 */
public class JdbcTransaction implements Transaction {
    private final static AtomicLong ID_GENERATOR = new AtomicLong(1);
    private boolean closed;
    private final long transactionId;
    private final Session session;
    private Map<String, CombinedSavepoint> savepoints;
    private ConnectionHolder connHolder;

    public JdbcTransaction(Session session) {
        this.session = session;
        this.transactionId = ID_GENERATOR.getAndIncrement();
        this.connHolder = new ConnectionHolder(session);
    }

    @Override
    public void setIsolation(final int level) {
        checkClosed();
        connHolder.foreach(new Callback<String>() {
            @Override
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.setTransactionIsolation(level);
                return shardName;
            }
        });

    }

    @Override
    public void setReadOnly(final boolean readOnly) {
        checkClosed();
        connHolder.foreach(new Callback<String>() {
            @Override
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.setReadOnly(readOnly);
                return shardName;
            }
        });

    }


    @Override
    public void commit() {
        checkClosed();
        connHolder.foreach(new Callback<String>() {
            @Override
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.commit();
                return shardName;
            }
        });
        connHolder.closeAndClear();
    }

    @Override
    public void rollback() {
        checkClosed();
        connHolder.foreach(new Callback<String>() {
            @Override
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.rollback();
                return shardName;
            }
        });
        connHolder.closeAndClear();
    }

    @Override
    public void addSavepoint(final String name) {
        checkClosed();
        if (savepoints == null) {
            savepoints = session.getDatabase().newStringMap();
        }
        final Map<String, Savepoint> binds = New.hashMap();
        connHolder.foreach(new Callback<String>() {
            @Override
            public String handle(String shardName, Connection connection) throws SQLException {
                Savepoint savepoint = connection.setSavepoint(name);
                binds.put(shardName, savepoint);
                return shardName;
            }
        });
        CombinedSavepoint sp = new CombinedSavepoint();
        sp.combined = binds;
        savepoints.put(name, sp);
    }

    @Override
    public void releaseSavepoint(String name) {
        checkClosed();
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        final CombinedSavepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        connHolder.foreach(savepoint.combined.keySet(), new Callback<String>() {
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.releaseSavepoint(savepoint.combined.get(shardName));
                return shardName;
            }
        });
        savepoints.remove(savepoint);
    }
    @Override
    public void rollbackToSavepoint(String name) {
        checkClosed();
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        final CombinedSavepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        connHolder.foreach(savepoint.combined.keySet(), new Callback<String>() {
            public String handle(String shardName, Connection connection) throws SQLException {
                connection.rollback(savepoint.combined.get(shardName));
                return shardName;
            }
        });
        savepoints.remove(savepoint);
    }

    public void checkClosed() {
        if (closed) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            connHolder.closeAndClear();
            closed = true;
        }
    }
    
    @Override
    public boolean isClosed() {
        return closed;
    }

    public ConnectionProvider getConnectionProvider() {
        return connHolder;
    }

    @Override
    public Long getId() {
        return transactionId;
    }

    
    public static class CombinedSavepoint {
        Map<String, Savepoint> combined;
    }

}
