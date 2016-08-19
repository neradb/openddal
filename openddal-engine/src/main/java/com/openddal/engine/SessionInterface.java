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
package com.openddal.engine;

import java.io.Closeable;
import java.sql.SQLException;

import com.openddal.command.CommandInterface;
import com.openddal.message.Trace;
import com.openddal.value.Value;

/**
 * A local or remote session. A session represents a database connection.
 */
public interface SessionInterface extends Closeable {
    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    CommandInterface prepareCommand(String sql, int fetchSize);

    /**
     * Roll back pending transactions and close the session.
     */
    @Override
    void close();

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    Trace getTrace();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    /**
     * Add a temporary LOB, which is closed when the session commits.
     *
     * @param v the value
     */
    void addTemporaryLob(Value v);

    /**
     * Check if this session is in auto-commit mode.
     *
     * @return true if the session is in auto-commit mode
     */
    boolean getAutoCommit();

    /**
     * Set the auto-commit mode. This call doesn't commit the current
     * transaction.
     *
     * @param autoCommit the new value
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Puts this connection in read-only mode as a hint to the driver to enable
     * database optimizations. This method cannot be called during a
     * transaction.
     * 
     * @param readOnly true enables read-only mode false disables it
     */
    void setReadOnly(boolean readOnly);

    /**
     * Retrieves whether this <code>Connection</code> object is in read-only
     * mode.
     * 
     * @return true if this session is read-only; false otherwise
     */
    boolean isReadOnly() throws SQLException;

    /**
     * Changes the current transaction isolation level. Calling this method will
     * commit an open transaction, even if the new level is the same as the old
     * one,except if the level is not supported.
     * 
     * @param level the new transaction isolation level:
     *            Connection.TRANSACTION_READ_UNCOMMITTED,
     *            Connection.TRANSACTION_READ_COMMITTED, or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @return
     */
    int getIsolation();

    /**
     * Returns the current transaction isolation level.
     *
     * @return the isolation level.
     */
    void setIsolation(int level);

    /**
     * Gets the current query timeout in millisecond. This method will return 0
     * if no query timeout is set.
     *
     * @return the timeout in millisecond
     */
    int getQueryTimeout();

    /**
     * Sets the current query timeout in millisecond.
     *
     * @param queryTimeout the timeout in millisecond - 0 means no timeout,
     *            values smaller 0 will throw an exception
     */
    void setQueryTimeout(int queryTimeout);

}
