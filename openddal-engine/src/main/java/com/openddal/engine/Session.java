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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import com.openddal.command.Command;
import com.openddal.command.CommandInterface;
import com.openddal.command.Parser;
import com.openddal.command.Prepared;
import com.openddal.dbobject.User;
import com.openddal.dbobject.index.Index;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.Table;
import com.openddal.engine.spi.Transaction;
import com.openddal.executor.ExecutorFactory;
import com.openddal.executor.works.WorkerFactory;
import com.openddal.executor.works.WorkerFactoryProxy;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.message.Trace;
import com.openddal.message.TraceSystem;
import com.openddal.result.LocalResult;
import com.openddal.util.New;
import com.openddal.util.SmallLRUCache;
import com.openddal.value.Value;
import com.openddal.value.ValueLong;
import com.openddal.value.ValueNull;
import com.openddal.value.ValueString;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class Session implements SessionInterface {

    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";
    private static int nextSerialId;

    private final int serialId = nextSerialId++;
    private final Database database;
    private final User user;
    private final int id;
    private final long sessionStart = System.currentTimeMillis();
    private final int queryCacheSize;
    private boolean autoCommit = true;
    private Random random;
    private Value lastIdentity = ValueLong.get(0);
    private Value lastScopeIdentity = ValueLong.get(0);
    private HashMap<String, Savepoint> savepoints;
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;

    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private int systemIdentifier;
    private volatile long cancelAt;
    private boolean closed;
    private long transactionStart;
    private long currentCommandStart;
    private HashMap<String, Value> variables;
    private HashSet<LocalResult> temporaryResults;
    private int queryTimeout;
    private int objectId;
    private SmallLRUCache<String, Command> queryCache;
    private ArrayList<Value> temporaryLobs;
    private boolean readOnly;
    private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
    private final Transaction transaction;
    private final WorkerFactoryProxy workerHolder;

    public Session(Database database, User user, int id) {
        this.id = id;
        this.user = user;
        this.database = database;
        this.queryTimeout = database.getSettings().defaultQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.currentSchemaName = Constants.SCHEMA_MAIN;
        this.transaction = database.getRepository().newTransaction(this);
        this.workerHolder = new WorkerFactoryProxy(this);
    }


    private void initVariables() {
        if (variables == null) {
            variables = database.newStringMap();
        }
    }

    /**
     * Set the value of the given variable for this session.
     *
     * @param name  the name of the variable (may not be null)
     * @param value the new value (may not be null)
     */
    public void setVariable(String name, Value value) {
        initVariables();
        Value old;
        if (value == ValueNull.INSTANCE) {
            old = variables.remove(name);
        } else {
            old = variables.put(name, value);
        }
        if (old != null) {
            // close the old value (in case it is a lob)
            old.close();
        }
    }

    /**
     * Get the value of the specified user defined variable. This method always
     * returns a value; it returns ValueNull.INSTANCE if the variable doesn't
     * exist.
     *
     * @param name the variable name
     * @return the value, or NULL
     */
    public Value getVariable(String name) {
        initVariables();
        Value v = variables.get(name);
        return v == null ? ValueNull.INSTANCE : v;
    }

    /**
     * Get the list of variable names that are set for this session.
     *
     * @return the list of names
     */
    public String[] getVariableNames() {
        if (variables == null) {
            return new String[0];
        }
        String[] list = new String[variables.size()];
        variables.keySet().toArray(list);
        return list;
    }

    /**
     * Get the local temporary table if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Table findLocalTempTable(String name) {
        if (localTempTables == null) {
            return null;
        }
        return localTempTables.get(name);
    }

    public ArrayList<Table> getLocalTempTables() {
        if (localTempTables == null) {
            return New.arrayList();
        }
        return New.arrayList(localTempTables.values());
    }

    /**
     * Add a local temporary table to this session.
     *
     * @param table the table to add
     * @throws DbException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) {
        if (localTempTables == null) {
            localTempTables = database.newStringMap();
        }
        if (localTempTables.get(table.getName()) != null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, table.getSQL());
        }
        localTempTables.put(table.getName(), table);
    }

    /**
     * Drop and remove the given local temporary table from this session.
     *
     * @param table the table
     */
    public void removeLocalTempTable(Table table) {
        localTempTables.remove(table.getName());
    }

    /**
     * Get the local temporary index if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Index findLocalTempTableIndex(String name) {
        if (localTempTableIndexes == null) {
            return null;
        }
        return localTempTableIndexes.get(name);
    }

    public HashMap<String, Index> getLocalTempTableIndexes() {
        if (localTempTableIndexes == null) {
            return New.hashMap();
        }
        return localTempTableIndexes;
    }

    /**
     * Add a local temporary index to this session.
     *
     * @param index the index to add
     * @throws DbException if a index with this name already exists
     */
    public void addLocalTempTableIndex(Index index) {
        if (localTempTableIndexes == null) {
            localTempTableIndexes = database.newStringMap();
        }
        if (localTempTableIndexes.get(index.getName()) != null) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, index.getSQL());
        }
        localTempTableIndexes.put(index.getName(), index);
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean b) {
        if (autoCommit == b) {
            return;
        }
        if (!getAutoCommit()) {
            commit();
        }
        autoCommit = b;
    }

    public User getUser() {
        return user;
    }

    @Override
    public synchronized CommandInterface prepareCommand(String sql, int fetchSize) {
        return prepareLocal(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the
     * rights.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) {
        return prepare(sql, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     *
     * @param sql           the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @return the prepared statement
     */
    public Prepared prepare(String sql, boolean rightsChecked) {
        Parser parser = new Parser(this);
        parser.setRightsChecked(rightsChecked);
        return parser.prepare(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks if the
     * connection has been closed.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Command prepareLocal(String sql) {
        if (closed) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
        Command command;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
                // modificationMetaID = database.getModificationMetaId();
            } else {
                // ignore table structure modification
                /*
                 * long newModificationMetaID =
                 * database.getModificationMetaId(); if (newModificationMetaID
                 * != modificationMetaID) { queryCache.clear();
                 * modificationMetaID = newModificationMetaID; }
                 */
                command = queryCache.get(sql);
                if (command != null && command.canReuse()) {
                    command.reuse();
                    return command;
                }
            }
        }
        Parser parser = new Parser(this);
        command = parser.prepareCommand(sql);
        if (queryCache != null) {
            if (command.isCacheable()) {
                queryCache.put(sql, command);
            }
        }
        return command;
    }

    public Database getDatabase() {
        return database;
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     *
     * @param ddl if the statement was a data definition statement
     */
    public void commit() {
        try {
            transaction.commit();
        } finally {
            endTransaction();
        }
    }
    
    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        try {
            transaction.rollback();
        } finally {
            endTransaction();
        }
    }

    private void endTransaction() {
        transactionStart = 0;
        savepoints = null;
        if (temporaryLobs != null) {
            for (Value v : temporaryLobs) {
                v.close();
            }
            temporaryLobs.clear();
        }
        
        if (localTempTables != null && localTempTables.size() > 0) {
            synchronized (database) {
                for (Table table : New.arrayList(localTempTables.values())) {
                    localTempTables.remove(table.getName());
                }
            }
        }
    }

    public int getId() {
        return id;
    }

    @Override
    public void cancel() {
        cancelAt = System.currentTimeMillis();
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (!getAutoCommit()) {
                    rollback();
                }
                transaction.close();
                database.removeSession(this);
            } finally {
                closed = true;
            }
        }
    }


    public Random getRandom() {
        if (random == null) {
            random = new Random();
        }
        return random;
    }

    @Override
    public Trace getTrace() {
        if (trace != null && !closed) {
            return trace;
        }
        if (closed) {
            return new TraceSystem().getTrace(Trace.JDBC);
        }
        trace = database.getTrace(Trace.JDBC);
        return trace;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
        this.lastScopeIdentity = last;
    }

    public Value getLastScopeIdentity() {
        return lastScopeIdentity;
    }

    public void setLastScopeIdentity(Value last) {
        this.lastScopeIdentity = last;
    }

    /**
     * Create a savepoint that is linked to the current log position.
     *
     * @param name the savepoint name
     */
    public void addSavepoint(final String name) {
        if (savepoints == null) {
            savepoints = database.newStringMap();
        }
        Savepoint sp = new Savepoint();
        sp.savepointName = name;
        transaction.addSavepoint(name);
        savepoints.put(name, sp);
    }
    
    /**
     * Releases a savepoint.
     *
     * @param savepoint the savepoint to release
     */
    public void releaseSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        final Savepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        if(transaction != null) {
            transaction.rollbackToSavepoint(name);
        }
        savepoints.remove(name);
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        final Savepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        if(transaction != null) {
            transaction.rollbackToSavepoint(name);
        }
        savepoints.remove(name);
    }


    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Check if the current transaction is canceled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     *
     * @throws DbException if the transaction is canceled
     */
    public void checkCanceled() {
        if (cancelAt == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if (time >= cancelAt) {
            cancelAt = 0;
            doCancel();
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    public void doCancel() {
        workerHolder.cancelWorkers();
    }

    /**
     * Get the cancel time.
     *
     * @return the time or 0 if not set
     */
    public long getCancel() {
        return cancelAt;
    }

    public Command getCurrentCommand() {
        return currentCommand;
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     *
     * @param command the command
     */
    public void setCurrentCommand(Command command) {
        this.currentCommand = command;
        if (queryTimeout > 0 && command != null) {
            long now = System.currentTimeMillis();
            currentCommandStart = now;
            cancelAt = now + queryTimeout;
        }
    }

    public long getCurrentCommandStart() {
        return currentCommandStart;
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        this.currentSchemaName = schema.getName();
    }

    public String getCurrentSchemaName() {
        return currentSchemaName;
    }

    /**
     * Get the next system generated identifiers. The identifier returned does
     * not occur within the given SQL statement.
     *
     * @param sql the SQL statement
     * @return the new identifier
     */
    public String getNextSystemIdentifier(String sql) {
        String identifier;
        do {
            identifier = SYSTEM_IDENTIFIER_PREFIX + systemIdentifier++;
        } while (sql.contains(identifier));
        return identifier;
    }

    public String[] getSchemaSearchPath() {
        return schemaSearchPath;
    }

    public void setSchemaSearchPath(String[] schemas) {
        this.schemaSearchPath = schemas;
    }

    @Override
    public int hashCode() {
        return serialId;
    }

    @Override
    public String toString() {
        return "#" + serialId + " (user: " + user.getName() + ")";
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        setAutoCommit(false);
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public long getTransactionStart() {
        if (transactionStart == 0) {
            transactionStart = System.currentTimeMillis();
        }
        return transactionStart;
    }

    /**
     * Remember the result set and close it as soon as the transaction is
     * committed (if it needs to be closed). This is done to delete temporary
     * files as soon as possible, and free object ids of temporary tables.
     *
     * @param result the temporary result set
     */
    public void addTemporaryResult(LocalResult result) {
        if (!result.needToClose()) {
            return;
        }
        if (temporaryResults == null) {
            temporaryResults = New.hashSet();
        }
        if (temporaryResults.size() < 100) {
            // reference at most 100 result sets to avoid memory problems
            temporaryResults.add(result);
        }
    }

    private void closeTemporaryResults() {
        if (temporaryResults != null) {
            for (LocalResult result : temporaryResults) {
                result.close();
            }
            temporaryResults = null;
        }
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        if (queryTimeout > 0) {
            this.queryTimeout = queryTimeout;
        }
        // must reset the cancel at here,
        // otherwise it is still used
        this.cancelAt = 0;
    }


    public Value getTransactionId() {
        if (transaction == null) {
            return ValueNull.INSTANCE;
        }
        String strId = Long.toString(transaction.getId());
        return ValueString.get(strId);
    }
    

    public Transaction getTransaction() {
        // Invoked by query thread, don't use the session object to sync,
        // the main thread locked session object on Command.executeXXX and
        // Statement.executeInternal,so transaction is final
        return transaction;
    }
    /**
     * Get the next object id.
     *
     * @return the next object id
     */
    public int nextObjectId() {
        return objectId++;
    }

    /**
     * Mark the statement as completed. This also close all temporary result
     * set, and deletes all temporary files held by the result sets.
     */
    public void endStatement() {
        workerHolder.closeWorkers();
        closeTemporaryResults();
    }

    @Override
    public void addTemporaryLob(Value v) {
        if (temporaryLobs == null) {
            temporaryLobs = new ArrayList<Value>();
        }
        temporaryLobs.add(v);
    }

    public int getIsolation() {
        return transactionIsolation;
    }

    public void setIsolation(int level) {
        if(transactionIsolation == level) {
            return;
        }
        switch (level) {
            case Connection.TRANSACTION_NONE:
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                break;
            default:
                throw DbException.getInvalidValueException("transaction isolation", level);
        }
        if(transaction != null) {
            transaction.setIsolation(level);
        }
        transactionIsolation = level;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        if(this.readOnly == readOnly) {
            return;
        }
        if(transaction != null) {
            transaction.setReadOnly(readOnly);
        }
        this.readOnly = readOnly;
    }
    
    public ExecutorFactory getExecutorFactory() {
        return database.getExecutorFactory();
    }

    public WorkerFactory getQueryHandlerFactory() {
        return workerHolder;
    }


    public static class Savepoint {
        String savepointName;
    }

}
