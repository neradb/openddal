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

import com.openddal.engine.Session;
import com.openddal.engine.spi.Transaction;
import com.openddal.message.Trace;
import com.openddal.repo.ConnectionProvider;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class JdbcTransaction implements Transaction {

    private final Session session;
    private final Trace trace;
    private final ConnectionHolder connectionHolder;


    public JdbcTransaction(Session session) {
        this.session = session;
        this.trace = session.getDatabase().getTrace(Trace.TRANSACTION);
        this.connectionHolder = new ConnectionHolder(session);
    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#setAutoCommit(boolean)
     */
    @Override
    public void setAutoCommit(boolean autoCommit) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#setIsolation(int)
     */
    @Override
    public void setIsolation(int level) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#setReadOnly(boolean)
     */
    @Override
    public void setReadOnly(boolean readOnly) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#begin()
     */
    @Override
    public void begin() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#commit()
     */
    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#rollback()
     */
    @Override
    public void rollback() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#addSavepoint(java.lang.String)
     */
    @Override
    public void addSavepoint(String name) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#rollbackToSavepoint(java.lang.String)
     */
    @Override
    public void rollbackToSavepoint(String name) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.openddal.engine.spi.Transaction#close()
     */
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    public ConnectionProvider getConnectionProvider() {
        return connectionHolder;
    }
}
