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
package com.openddal.repo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.openddal.config.Configuration;
import com.openddal.config.DataSourceException;
import com.openddal.config.DataSourceProvider;
import com.openddal.config.SequenceRule;
import com.openddal.config.Shard;
import com.openddal.config.Shard.ShardItem;
import com.openddal.config.TableRule;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.engine.spi.Repository;
import com.openddal.engine.spi.Transaction;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.message.Trace;
import com.openddal.repo.ha.DataSourceMarker;
import com.openddal.repo.ha.Failover;
import com.openddal.repo.ha.SmartDataSource;
import com.openddal.repo.tx.JdbcTransaction;
import com.openddal.util.JdbcUtils;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.util.Threads;

/**
 * @author jorgie.li
 */
public abstract class JdbcRepository implements Repository, ConnectionProvider {

    private final List<DataSourceMarker> registered = New.arrayList();
    private final List<DataSourceMarker> abnormalList = New.copyOnWriteArrayList();
    private final List<DataSourceMarker> monitor = New.copyOnWriteArrayList();

    private final HashMap<String, DataSource> shardMaping = New.hashMap();
    private final HashMap<String, DataSource> idMapping = New.hashMap();

    private Database database;
    private String defaultShardName;
    private DataSourceProvider dataSourceProvider;
    private Trace trace;
    private String validationQuery;
    private int validationQueryTimeout;
    private ScheduledExecutorService scheduledExecutor;

    public void init(Database database) {
        // database not init completed
        this.database = database;
        Configuration configuration = database.getConfiguration();
        this.defaultShardName = configuration.publicDB;
        this.validationQuery = database.getSettings().validationQuery;
        this.validationQueryTimeout = database.getSettings().validationQueryTimeout;
        this.dataSourceProvider = configuration.provider;
        if (dataSourceProvider == null) {
            throw new IllegalArgumentException();
        }
        this.trace = database.getTrace(Trace.REPOSITORY);
        for (Shard shardItem : configuration.cluster) {
            List<ShardItem> shardItems = shardItem.getShardItems();
            List<DataSourceMarker> shardDs = New.arrayList(shardItems.size());
            DataSourceMarker dsMarker = new DataSourceMarker();
            for (ShardItem i : shardItems) {
                String ref = i.getRef();
                DataSource dataSource = dataSourceProvider.lookup(ref);
                if (dataSource == null) {
                    throw new DataSourceException("Can' find data source: " + ref);
                }
                dsMarker.setDataSource(dataSource);
                dsMarker.setShardName(shardItem.getName());
                dsMarker.setUid(ref);
                dsMarker.setReadOnly(i.isReadOnly());
                dsMarker.setwWeight(i.getwWeight());
                dsMarker.setrWeight(i.getrWeight());
                shardDs.add(dsMarker);
                idMapping.put(ref, dsMarker.getDataSource());
            }
            if (shardDs.size() < 1) {
                throw new DataSourceException("No datasource in " + shardItem.getName());
            }
            registered.addAll(shardDs);
            DataSource dataSource = shardDs.size() > 1 ? new SmartDataSource(this, shardItem.getName(), shardDs)
                    : shardDs.get(0).getDataSource();
            shardMaping.put(shardItem.getName(), dataSource);
        }
        scheduledExecutor = Executors.newScheduledThreadPool(1, Threads.newThreadFactory("datasource-ha-thread"));
        scheduledExecutor.scheduleAtFixedRate(new Worker(), 10, 10, TimeUnit.SECONDS);
    }

    public DataSource getDataSourceByShardName(String shardName) {
        DataSource dataSource = shardMaping.get(shardName);
        if (dataSource == null) {
            throw new IllegalArgumentException(shardName + " DataSource not found.");
        }
        return dataSource;
    }

    public DataSource getDataSourceById(String id) {
        DataSource dataSource = idMapping.get(id);
        if (dataSource == null) {
            throw new IllegalArgumentException();
        }
        return dataSource;
    }

    /**
     * @return the validationQuery
     */
    public String getValidationQuery() {
        return validationQuery;
    }

    /**
     * @param validationQuery the validationQuery to set
     */
    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    /**
     * @return the validationQueryTimeout
     */
    public int getValidationQueryTimeout() {
        return validationQueryTimeout;
    }

    /**
     * @param validationQueryTimeout the validationQueryTimeout to set
     */
    public void setValidationQueryTimeout(int validationQueryTimeout) {
        this.validationQueryTimeout = validationQueryTimeout;
    }

    public Database getDatabase() {
        return database;
    }
    
    public Trace getTrace() {
        return trace;
    }

    public int shardCount() {
        return this.shardMaping.size();
    }

    @Override
    public String getPublicDB() {
        return defaultShardName;
    }

    public DataSource getDefaultShardDataSource() {
        if (StringUtils.isNullOrEmpty(defaultShardName)) {
            return null;
        }
        return shardMaping.get(defaultShardName);
    }

    public void close() {
        if (scheduledExecutor != null) {
            Threads.shutdownGracefully(scheduledExecutor, 1000, 1000, TimeUnit.MILLISECONDS);
        }
    }

    public Connection haGet(DataSourceMarker selected) throws SQLException {
        DataSource dataSource = selected.getDataSource();
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            selected.incrementFailedCount();
            monitor.add(selected);
            throw e;
        }

    }

    public Connection haGet(DataSourceMarker selected, String username, String password) throws SQLException {
        DataSource dataSource = selected.getDataSource();
        try {
            return dataSource.getConnection(username, password);
        } catch (SQLException e) {
            selected.incrementFailedCount();
            monitor.add(selected);
            throw e;
        }

    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                handleMonitorList();
            } catch (Exception e) {
                trace.error(e, "datasource-ha-thread handle monitor list error");
            }
            try {
                hanldeAbnormalList();
            } catch (Exception e) {
                trace.error(e, "datasource-ha-thread handle monitor list error");
            }
        }

        /**
         * @throws SQLException
         */
        private void hanldeAbnormalList() throws SQLException {
            for (DataSourceMarker failed : abnormalList) {
                DataSource ds = failed.getDataSource();
                boolean isOk = validateAvailable(ds);
                if (isOk) {
                    DataSource dataSource = shardMaping.get(failed.getShardName());
                    Failover selector = (Failover) dataSource;
                    selector.doHandleWakeup(failed);
                    abnormalList.remove(failed);
                }

            }
        }

        /**
         * @throws SQLException
         */
        private void handleMonitorList() throws SQLException {
            for (DataSourceMarker source : monitor) {
                DataSource ds = source.getDataSource();
                boolean isOk = validateAvailable(ds);
                if (!isOk) {
                    DataSource dataSource = shardMaping.get(source.getShardName());
                    Failover selector = (Failover) dataSource;
                    selector.doHandleAbnormal(source);
                    abnormalList.add(source);
                    trace.error(null, source.toString() + " was abnormal,it's remove in " + source.getShardName());
                }
                monitor.remove(source);
            }
        }

        private boolean validateAvailable(DataSource dataSource) throws SQLException {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
            } catch (SQLException ex) {
                // skip
                return false;
            }
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                if (validationQueryTimeout > 0) {
                    stmt.setQueryTimeout(validationQueryTimeout);
                } else {
                    stmt.setQueryTimeout(5);
                }
                rs = stmt.executeQuery(validationQuery);
                return true;
            } catch (SQLException e) {
                return false;
            } catch (Exception e) {
                // LOG.warn("Unexpected error in ping", e);
                return false;
            } finally {
                JdbcUtils.closeSilently(rs);
                JdbcUtils.closeSilently(stmt);
            }

        }

    }

    @Override
    public boolean isAsyncSupported() {
        return false;
    }

    @Override
    public TableMate loadMataData(Schema schema, TableRule tableRule) {
        String identifier = database.identifier(tableRule.getName());
        TableMate tableMate = new TableMate(schema, identifier, tableRule);
        return tableMate;
    }

    @Override
    public Sequence loadMataData(Schema schema, SequenceRule sequenceRule) {
        String strategy = sequenceRule.getStrategy();
        String name = database.identifier(sequenceRule.getName());
        if ("hilo".equals(strategy)) {
            return new TableHiLoGenerator(schema, name, sequenceRule);
        } else if ("snowflake".equals(strategy)) {
            return new SnowflakeGenerator(schema, name, sequenceRule);
        } else {
            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, strategy + " sequence");
        }
    }

    public ConnectionProvider getConnectionProvider() {
        return this;
    }
    

    @Override
    public Connection getConnection(Options options) {
        DataSource ds = getDataSourceByShardName(options.shardName);
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void closeConnection(Connection connection, Options options) {
        if(connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }


    @Override
    public Transaction newTransaction(Session session) {
        return new JdbcTransaction(session);
    }
    

    public abstract SQLTranslator getSQLTranslator();


}
