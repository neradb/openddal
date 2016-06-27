package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.Trace;
import com.openddal.repo.ConnectionProvider;
import com.openddal.repo.JdbcRepository;
import com.openddal.repo.Options;
import com.openddal.util.New;

public class ConnectionHolder implements ConnectionProvider {

    private final Session session;
    private final ConnectionProvider target;
    private final Trace trace;
    private Map<String, Connection> connectionMap = New.hashMap();
    private final Closer closer = new Closer();
    
    public ConnectionHolder(Session session) {
        this.session = session;
        this.trace = session.getDatabase().getTrace(Trace.TRANSACTION);
        JdbcRepository repository = (JdbcRepository) session.getDatabase().getRepository();
        this.target = repository.getConnectionProvider();
    }

    public synchronized <T> List<T> foreach(Callback<T> callback) throws DbException {
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

    public synchronized <T> List<T> foreach(Set<String> shards, Callback<T> callback) throws DbException {
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
    public synchronized Connection getConnection(Options options) {
        Connection conn;
        if (session.getAutoCommit()) {
            conn = getRawConnection(options);
        } else {
            conn = connectionMap.get(options.shardName);
            if (conn == null) {
                conn = getRawConnection(options);
                connectionMap.put(options.shardName, conn);
            }
        }
        return conn;
    }

    @Override
    public synchronized void closeConnection(Connection connection, Options options) {
        if (!connectionMap.containsKey(options.shardName)) {
            target.closeConnection(connection, options);
        }
    }

    public synchronized boolean hasConnection() {
        return !connectionMap.isEmpty();
    }

    public synchronized List<String> closeAndClear() {
        List<String> foreach = foreach(closer);
        connectionMap.clear();
        return foreach;
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
            if (session.getTransactionIsolation() != 0) {
                if (conn.getTransactionIsolation() != session.getTransactionIsolation()) {
                    conn.setTransactionIsolation(session.getTransactionIsolation());
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
                //throw DbException.convert(e);
            }
            return name;
        }

    };

}
