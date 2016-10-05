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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.openddal.config.Configuration;
import com.openddal.config.SequenceRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.DbObject;
import com.openddal.dbobject.User;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.SchemaObject;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.dbobject.table.MetaTable;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.spi.Repository;
import com.openddal.executor.ExecutorFactory;
import com.openddal.executor.ExecutorFactoryImpl;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.message.Trace;
import com.openddal.message.TraceSystem;
import com.openddal.route.RoutingHandler;
import com.openddal.route.RoutingHandlerImpl;
import com.openddal.util.BitField;
import com.openddal.util.ExtendableThreadPoolExecutor;
import com.openddal.util.ExtendableThreadPoolExecutor.TaskQueue;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.util.Threads;
import com.openddal.value.CaseInsensitiveMap;
import com.openddal.value.CompareMode;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class Database {

    public static final String SYSTEM_USER_NAME = "MASTER";

    private final HashMap<String, User> users = New.hashMap();
    private final HashMap<String, Schema> schemas = New.hashMap();

    private final Set<Session> userSessions = Collections.synchronizedSet(new HashSet<Session>());

    private final BitField objectIds = new BitField();
    private final DbSettings dbSettings;
    private int nextSessionId;
    private TraceSystem traceSystem;
    private Trace trace;
    private CompareMode compareMode;
    private int allowLiterals = Constants.ALLOW_LITERALS_ALL;
    private volatile boolean closing;
    private boolean ignoreCase;// for data type VARCHAR_IGNORECASE
    private Mode mode = Mode.getInstance(Mode.REGULAR);
    private int maxMemoryRows = SysProperties.MAX_MEMORY_ROWS;
    private int maxOperationMemory = Constants.DEFAULT_MAX_OPERATION_MEMORY;
    private boolean queryStatistics;
    private int queryStatisticsMaxEntries = Constants.QUERY_STATISTICS_MAX_ENTRIES;
    private QueryStatisticsData queryStatisticsData;
    private RoutingHandler routingHandler;
    private final ThreadPoolExecutor queryExecutor;
    private final Repository repository;
    private final ExecutorFactory executorFactory;
    private final Configuration configuration;

    public Database(Configuration configuration) {
        this.configuration = configuration;
        this.compareMode = CompareMode.getInstance(null, 0);
        this.dbSettings = getDbSettings(configuration.settings);

        this.mode = Mode.getInstance(dbSettings.sqlMode);
        this.traceSystem = new TraceSystem();
        this.trace = traceSystem.getTrace(Trace.DATABASE);

        this.queryExecutor = createQueryExecutor();
        this.repository = bindRepository();
        this.executorFactory = new ExecutorFactoryImpl();
        openDatabase();
    }

    private ExtendableThreadPoolExecutor createQueryExecutor() {
        TaskQueue queue = new TaskQueue(SysProperties.THREAD_QUEUE_SIZE);
        int poolCoreSize = SysProperties.THREAD_POOL_SIZE_CORE;
        int poolMaxSize = SysProperties.THREAD_POOL_SIZE_MAX;
        poolMaxSize = poolMaxSize > poolCoreSize ? poolMaxSize : poolCoreSize;
        ExtendableThreadPoolExecutor queryExecutor = new ExtendableThreadPoolExecutor(poolCoreSize, poolMaxSize, 2L,
                TimeUnit.MINUTES, queue, Threads.newThreadFactory("ddal-query-executor"));
        return queryExecutor;
    }

    private Repository bindRepository() {
        ServiceLoader<Repository> serviceLoader = ServiceLoader.load(Repository.class);
        Map<String,Repository> repositories = New.hashMap();
        for (Repository service : serviceLoader) {
            trace.debug("Repository {0} was found on the class path.", service.getClass().getName());
            repositories.put(service.getName(), service);
        }
        if (repositories.isEmpty()) {
            throw DbException.get(ErrorCode.REPOSITORY_BINDING_ERROR_1);
        }
        if (repositories.size() > 1) {
            String names = repositories.keySet().toString();
            throw DbException.get(ErrorCode.REPOSITORY_BINDING_ERROR_2, names);
        }
        Repository repository = repositories.values().iterator().next();
        repository.init(this);
        return repository;
    }

    private synchronized void openDatabase() {
        User systemUser = new User(this, SYSTEM_USER_NAME);
        systemUser.setAdmin(true);
        systemUser.setPassword(SYSTEM_USER_NAME);
        users.put(SYSTEM_USER_NAME, systemUser);

        Schema mainSchema = new Schema(this, Constants.SCHEMA_MAIN, systemUser, true);
        Schema infoSchema = new Schema(this, "INFORMATION_SCHEMA", systemUser, true);
        schemas.put(mainSchema.getName(), mainSchema);
        schemas.put(infoSchema.getName(), infoSchema);

        Session sysSession = createSession(systemUser);
        try {
            for (TableRule tableRule : configuration.tableRules) {
                TableMate tableMate = repository.loadMataData(mainSchema, tableRule);
                tableMate.loadMataData(sysSession);
                if (configuration.forceLoadTableMate) {
                    tableMate.check();
                }
                this.addSchemaObject(tableMate);
            }

            for (int type = 0, count = MetaTable.getMetaTableTypeCount(); type < count; type++) {
                MetaTable m = new MetaTable(infoSchema, type);
                infoSchema.add(m);
            }

            /*
             * for (TableRule tableRule : tableMates) { String identifier =
             * tableRule.getName(); identifier = identifier(identifier); Index
             * index = new Index(getTableOrViewByName(name), identifier,
             * newIndexColumns, newIndexType); this.addSchemaObject(index); }
             */

            for (SequenceRule config : configuration.sequnces) {
                Sequence sequence = repository.loadMataData(mainSchema, config);
                this.addSchemaObject(sequence);
            }

        } finally {
            sysSession.close();
        }

    }
    
    public DbSettings getDbSettings(Properties setting) {
        DbSettings defaultSettings = DbSettings.getDefaultSettings();
        HashMap<String, String> s = New.hashMap();
        for (Object k : setting.keySet()) {
            String key = k.toString();
            if (!defaultSettings.containsKey(key)) {
                throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, key);
            }
            s.put(key, setting.getProperty(key));
        }
        return DbSettings.getInstance(s);
    }

    /**
     * Check if two values are equal with the current comparison mode.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both objects are equal
     */
    public boolean areEqual(Value a, Value b) {
        // can not use equals because ValueDecimal 0.0 is not equal to 0.00.
        return a.compareTo(b, compareMode) == 0;
    }

    /**
     * Compare two values with the current comparison mode. The values may not
     * be of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compare(Value a, Value b) {
        return a.compareTo(b, compareMode);
    }

    /**
     * Compare two values with the current comparison mode. The values must be
     * of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSave(Value a, Value b) {
        return a.compareTypeSave(b, compareMode);
    }

    /**
     * Get the trace object for the given module.
     *
     * @param module the module name
     * @return the trace object
     */
    public Trace getTrace(String module) {
        return traceSystem.getTrace(module);
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, DbObject> getMap(int type) {
        HashMap<String, ? extends DbObject> result;
        switch (type) {
        case DbObject.USER:
            result = users;
            break;
        case DbObject.SCHEMA:
            result = schemas;
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        return (HashMap<String, DbObject>) result;
    }

    /**
     * Add a schema object to the database.
     *
     * @param obj the object to add
     */
    public synchronized void addSchemaObject(SchemaObject obj) {
        obj.getSchema().add(obj);
        // trace.debug("addSchemaObject: {0}", obj.getCreateSQL());
    }

    /**
     * Add an object to the database.
     *
     * @param obj the object to add
     */
    public synchronized void addDatabaseObject(DbObject obj) {
        HashMap<String, DbObject> map = getMap(obj.getType());
        String name = obj.getName();
        if (SysProperties.CHECK && map.get(name) != null) {
            DbException.throwInternalError("object already exists");
        }
        map.put(name, obj);
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(String schemaName) {
        Schema schema = schemas.get(schemaName);
        return schema;
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public User findUser(String name) {
        return users.get(name);
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public User getUser(String name) {
        User user = findUser(name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    /**
     * Create a session for the given user.
     *
     * @param user the user
     * @return the session
     * @throws DbException if the database is in exclusive mode
     */
    public synchronized Session createSession(User user) {
        Session session = new Session(this, user, ++nextSessionId);
        userSessions.add(session);
        if (trace.isDebugEnabled()) {
            trace.debug("create session id[{0}]", session.getId(), "engine");
        }
        return session;
    }

    /**
     * Remove a session. This method is called after the user has disconnected.
     *
     * @param session the session
     */
    public synchronized void removeSession(Session session) {
        if (session != null) {
            userSessions.remove(session);
        }
    }

    /**
     * Immediately close the database.
     */
    public void shutdownImmediately() {
        close();
    }

    /**
     * Close the database.
     */
    public synchronized void close() {
        if (closing) {
            return;
        }
        if (userSessions.size() > 0) {
            Session[] all = new Session[userSessions.size()];
            userSessions.toArray(all);
            for (Session s : all) {
                try {
                    s.rollback();
                    s.close();
                } catch (DbException e) {
                    trace.error(e, "disconnecting session #{0}", s.getId());
                }
            }
        }
        repository.close();
        if (queryExecutor != null) {
            Threads.shutdownGracefully(queryExecutor, 1000, 1000, TimeUnit.MILLISECONDS);
        }
        closing = true;
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public synchronized int allocateObjectId() {
        int i = objectIds.nextClearBit(0);
        objectIds.set(i);
        return i;
    }

    public int getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(int value) {
        this.allowLiterals = value;
    }

    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        ArrayList<SchemaObject> list = New.arrayList();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAll());
        }
        return list;
    }

    /**
     * Get all schema objects of the given type.
     *
     * @param type the int type
     */
    public ArrayList<SchemaObject> getAllSchemaObjects(int type) {
        ArrayList<SchemaObject> list = New.arrayList();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAll(type));
        }
        return list;
    }

    /**
     * Get all tables and views.
     *
     * @return all objects of that type
     */
    public ArrayList<Table> getAllTablesAndViews() {
        ArrayList<Table> list = New.arrayList();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    /**
     * Get the tables with the given name, if any.
     *
     * @param name the table name
     * @return the list
     */
    public ArrayList<Table> getTableOrViewByName(String name) {
        ArrayList<Table> list = New.arrayList();
        for (Schema schema : schemas.values()) {
            Table table = schema.getTableOrViewByName(name);
            if (table != null) {
                list.add(table);
            }
        }
        return list;
    }

    public ArrayList<Schema> getAllSchemas() {
        return New.arrayList(schemas.values());
    }

    public ArrayList<User> getAllUsers() {
        return New.arrayList(users.values());
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    /**
     * Get all sessions that are currently connected to the database.
     */
    public Session[] getSessions() {
        ArrayList<Session> list;
        // need to synchronized on userSession, otherwise the list
        // may contain null elements
        synchronized (userSessions) {
            list = New.arrayList(userSessions);
        }
        Session[] array = new Session[list.size()];
        list.toArray(array);
        return array;
    }

    /**
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) {
        obj.getSchema().rename(obj, newName);
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) {
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                DbException.throwInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                DbException.throwInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        map.remove(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
    }

    /**
     * Get the schema. If the schema does not exist, an exception is thrown.
     *
     * @param schemaName the name of the schema
     * @return the schema
     * @throws DbException no schema with that name exists
     */
    public Schema getSchema(String schemaName) {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public synchronized void removeDatabaseObject(Session session, DbObject obj) {
        String objName = obj.getName();
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            DbException.throwInternalError("not found: " + objName);
        }
        map.remove(objName);
    }

    /**
     * Remove an object from the system table.
     *
     * @param session the session
     * @param obj the object to be removed
     */
    public synchronized void removeSchemaObject(Session session, SchemaObject obj) {
        int type = obj.getType();
        if (type == DbObject.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.isTemporary()) {
                session.removeLocalTempTable(table);
                return;
            }
        }
        obj.getSchema().remove(obj);
    }

    public TraceSystem getTraceSystem() {
        return traceSystem;
    }

    /**
     * Check if the database is in the process of closing.
     *
     * @return true if the database is closing
     */
    public boolean isClosing() {
        return closing;
    }

    public boolean getIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean b) {
        ignoreCase = b;
    }

    public int getSessionCount() {
        return userSessions.size();
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
    }

    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory = maxOperationMemory;
    }

    public int getMaxMemoryRows() {
        return maxMemoryRows;
    }

    public void setMaxMemoryRows(int value) {
        this.maxMemoryRows = value;
    }

    public DbSettings getSettings() {
        return dbSettings;
    }

    public void setQueryStatistics(boolean b) {
        queryStatistics = b;
        synchronized (this) {
            queryStatisticsData = null;
        }
    }

    public boolean getQueryStatistics() {
        return queryStatistics;
    }

    public void setQueryStatisticsMaxEntries(int n) {
        queryStatisticsMaxEntries = n;
        if (queryStatisticsData != null) {
            synchronized (this) {
                if (queryStatisticsData != null) {
                    queryStatisticsData.setMaxQueryEntries(queryStatisticsMaxEntries);
                }
            }
        }
    }

    public QueryStatisticsData getQueryStatisticsData() {
        if (!queryStatistics) {
            return null;
        }
        if (queryStatisticsData == null) {
            synchronized (this) {
                if (queryStatisticsData == null) {
                    queryStatisticsData = new QueryStatisticsData(queryStatisticsMaxEntries);
                }
            }
        }
        return queryStatisticsData;
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @return the hash map
     */
    public <V> HashMap<String, V> newStringMap() {
        return dbSettings.databaseToUpper ? new HashMap<String, V>() : new CaseInsensitiveMap<V>();
    }

    /**
     * Compare two identifiers (table names, column names,...) and verify they
     * are equal. Case sensitivity depends on the configuration.
     *
     * @param a the first identifier
     * @param b the second identifier
     * @return true if they match
     */
    public boolean equalsIdentifiers(String a, String b) {
        if (a == b || a.equals(b)) {
            return true;
        }
        return !dbSettings.databaseToUpper && a.equalsIgnoreCase(b);
    }

    /**
     * String to database identifier against dbSettings
     *
     * @param identifier
     * @return
     */
    public String identifier(String identifier) {
        identifier = dbSettings.databaseToUpper ? StringUtils.toUpperEnglish(identifier) : identifier;
        return identifier;
    }

    /**
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    public RoutingHandler getRoutingHandler() {
        if (routingHandler == null) {
            routingHandler = new RoutingHandlerImpl(this);
        }
        return routingHandler;
    }

    public Repository getRepository() {
        return repository;
    }

    public ThreadPoolExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public ExecutorFactory getExecutorFactory() {
        return executorFactory;
    }
    
    

}
