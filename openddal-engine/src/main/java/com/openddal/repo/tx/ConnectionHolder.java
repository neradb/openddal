package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.message.Trace;
import com.openddal.repo.ConnectionProvider;
import com.openddal.repo.JdbcRepository;
import com.openddal.repo.Options;
import com.openddal.util.JdbcUtils;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

public class ConnectionHolder implements ConnectionProvider {

    private final Session session;
    private final HolderStrategy holderStrategy;
    private final ConnectionProvider target;
    private final Trace trace;
    private ConcurrentMap<String, Connection> connectionMap;
    private final Closer closer = new Closer();

    public ConnectionHolder(Session session) {
        Database database = session.getDatabase();
        JdbcRepository repository = (JdbcRepository) database.getRepository();
        this.session = session;
        this.trace = database.getTrace(Trace.TRANSACTION);
        this.target = repository.getConnectionProvider();
        String mode = database.getSettings().transactionMode;
        this.holderStrategy = transactionMode(mode);
        connectionMap = New.concurrentHashMap();
    }

    public <T> List<T> foreach(Callback<T> callback) throws DbException {
        List<T> results = New.arrayList();
        for (String name : connectionMap.keySet()) {
            try {
                Connection conn = connectionMap.get(name);
                results.add(callback.handle(name, conn));
            } catch (SQLException e) {
                trace.error(e, "foreach {0} connection error", name);
                throw DbException.convert(e);
            }
        }
        return results;
    }

    public <T> List<T> foreach(Set<String> shards, Callback<T> callback) throws DbException {
        List<T> results = New.arrayList();
        for (String name : connectionMap.keySet()) {
            if (shards.contains(name)) {
                try {
                    Connection conn = connectionMap.get(name);
                    results.add(callback.handle(name, conn));
                } catch (SQLException e) {
                    trace.error(e, "foreach {0} connection error", name);
                    throw DbException.convert(e);
                }
            }
        }
        return results;
    }

    @Override
    public Connection getConnection(Options options) {
        Connection conn;
        if (session.getAutoCommit()) {
            conn = getRawConnection(options);
        } else {
            conn = getConnectionWithStrategy(options);
        }
        return conn;
    }

    @Override
    public void closeConnection(Connection connection, Options options) {
        if (session.getAutoCommit()) {
            target.closeConnection(connection, options);
            
        } else {
            Connection contains = connectionMap.get(options.shardName);
            if (connection != contains) {
                target.closeConnection(connection, options);
            }
        }
    }

    public boolean hasConnection() {
        return !connectionMap.isEmpty();
    }

    public List<String> closeAndClear() {
        List<String> foreach = foreach(closer);
        connectionMap.clear();
        return foreach;
    }

    /**
     * Each shard have a database connection, the worker thread may concurrently
     * use a shard connection executing SQL (such as executing ddl statement),
     * If the JDBC driver is spec-compliant, then technically yes, the object is
     * thread-safe, MySQL Connector/J, all methods to execute statements lock
     * the connection with a synchronized block.
     * 
     * @param options
     * @return
     */
    protected Connection getConnectionWithStrategy(Options options) {
        String shardName = options.shardName;
        Connection conn;
        switch (holderStrategy) {
        case STRICTLY:
            if (connectionMap.isEmpty()) {
                conn = getRawConnection(options);
                Connection putIfAbsent = connectionMap.putIfAbsent(shardName, conn);
                if (putIfAbsent != null) {
                    JdbcUtils.closeSilently(conn);
                    conn = putIfAbsent;
                }
            } else {
                conn = connectionMap.get(shardName);
                if (conn == null) {
                    String lastTransactionNode = connectionMap.keySet().iterator().next();
                    throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                            "STRICTLY transaction mode not supported operation on multi-node, opend on "
                                    + lastTransactionNode + " and try on " + shardName);
                }
            }
            break;
        case ALLOW_CROSS_SHARD_READ:
            if(options.readOnly) {
                conn = connectionMap.get(shardName);
                if(conn == null) {
                    conn = getRawConnectionForReadOnly(options);
                }
            } else {
                if (connectionMap.isEmpty()) {
                    conn = getRawConnection(options);
                    Connection putIfAbsent = connectionMap.putIfAbsent(shardName, conn);
                    if (putIfAbsent != null) {
                        JdbcUtils.closeSilently(conn);
                        conn = putIfAbsent;
                    }
                } else {
                    conn = connectionMap.get(shardName);
                    if (conn == null) {
                        String lastTransactionNode = connectionMap.keySet().iterator().next();
                        throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                                "ALLOW_READ_CROSS_DB transaction mode not supported writing operation on multi-node, opend on "
                                        + lastTransactionNode + " and try on " + shardName);
                    }
                }
            }
            
            break;

        case BESTEFFORTS_1PC:
            conn = connectionMap.get(shardName);
            if (conn == null) {
                conn = getRawConnection(options);
                Connection putIfAbsent = connectionMap.putIfAbsent(shardName, conn);
                if (putIfAbsent != null) {
                    JdbcUtils.closeSilently(conn);
                    conn = putIfAbsent;
                }
            }
            break;

        default:
            throw DbException.getInvalidValueException("transactionMode", holderStrategy);
        }
        return conn;

    }

    /**
     * @param options
     * @return
     * @throws SQLException
     */
    private Connection getRawConnection(Options options) throws DbException {
        Connection conn = target.getConnection(options);
        try {
            if (conn.getAutoCommit() != session.getAutoCommit()) {
                conn.setAutoCommit(session.getAutoCommit());
            }
            if (session.getIsolation() != 0) {
                if (conn.getTransactionIsolation() != session.getIsolation()) {
                    conn.setTransactionIsolation(session.getIsolation());
                }
            }
            if (conn.isReadOnly() != session.isReadOnly()) {
                conn.setReadOnly(session.isReadOnly());
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return conn;
    }
    
    private Connection getRawConnectionForReadOnly(Options options) {
        try {
            Connection conn = target.getConnection(options);
            conn.setAutoCommit(true);
            conn.setReadOnly(true);
            if (session.getIsolation() != 0) {
                if (conn.getTransactionIsolation() != session.getIsolation()) {
                    conn.setTransactionIsolation(session.getIsolation());
                }
            }
            return conn;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private HolderStrategy transactionMode(String mode) {
        try {
            HolderStrategy holderStrategy = StringUtils.isNullOrEmpty(mode) ? HolderStrategy.BESTEFFORTS_1PC
                    : HolderStrategy.valueOf(mode);
            return holderStrategy;
        } catch (Exception e) {
            throw DbException.getInvalidValueException("transactionMode", mode);
        }
    }

    public static interface Callback<T> {
        T handle(String shardName, Connection connection) throws SQLException;
    }

    private class Closer implements Callback<String> {
        @Override
        public String handle(String name, Connection connection) throws SQLException {
            try {
                target.closeConnection(connection, Options.build().shardName(name));
            } catch (Exception e) {
                trace.error(e, "Close {0} connection error", name);
                // throw DbException.convert(e);
            }
            return name;
        }

    };

    enum HolderStrategy {
        STRICTLY, ALLOW_CROSS_SHARD_READ, BESTEFFORTS_1PC
    }

}
