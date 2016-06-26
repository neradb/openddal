package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
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
import com.openddal.util.StringUtils;

public class ConnectionHolder implements ConnectionProvider {

    private final Session session;
    private final ConnectionProvider target;
    private final Trace trace;
    private Map<String, Connection> connectionMap = New.hashMap();
    private final Closer closer = new Closer();
    private final Rollbacker rollbacker = new Rollbacker();
    private final Commiter commiter = new Commiter();
    
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
                    throw DbException.convert(e);
                }
            }
        }
        return results;
    }

    

    @Override
    public synchronized Connection getConnection(Options options) {
        Connection conn;
        if (!session.getAutoCommit()) {
            conn = connectionMap.get(options.shardName);
            if (conn == null) {
                conn = getRawConnection(options);
                connectionMap.put(options.shardName, conn);
            }
        } else {
            conn = getRawConnection(options);
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

    public synchronized List<String> commitAndClear() {
        List<String> foreach = foreach(commiter);
        connectionMap.clear();
        return foreach;
    }

    public synchronized List<String> rollbackAndClear() {
        List<String> foreach = foreach(rollbacker);
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

    public Closer getCloser() {
        return closer;
    }

    public Rollbacker getRollbacker() {
        return rollbacker;
    }

    public Callback<String> getCommiter() {
        return commiter;
    }

    public Callback<String> setReadOnly(boolean readOnly) {
        return new ReadOnly(readOnly);
    }

    public Callback<String> setAutoCommit(boolean autoCommit) {
        return new AutoCommit(autoCommit);
    }

    public Callback<String> setIsolation(int level) {
        return new Isolation(level);
    }

    public Callback<Savepoint> setSavePoint(String name) {
        return new SavePoint(name);
    }


    public static interface Callback<T> {
        T handle(String shardName, Connection connection) throws SQLException;
    }

    private class ReadOnly implements Callback<String> {
        private final boolean readOnly;

        public ReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
        }

        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                connection.setReadOnly(readOnly);
            } catch (SQLException e) {
                trace.error(e, "setAutoCommit {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }
    };

    private class AutoCommit implements Callback<String> {
        private final boolean autoCommit;
        /**
         * @param level
         */
        public AutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
        }

        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                trace.error(e, "setAutoCommit {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }
    };

    private class Isolation implements Callback<String> {
        private final int level;
        /**
         * @param level
         */
        public Isolation(int level) {
            this.level = level;
        }

        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                connection.setTransactionIsolation(level);
            } catch (SQLException e) {
                trace.error(e, "setTransactionIsolation {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }

    };
    private class Commiter implements Callback<String> {
        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                connection.commit();
            } catch (SQLException e) {
                trace.error(e, "Commit {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }

    };

    private class Rollbacker implements Callback<String> {
        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                connection.rollback();
            } catch (SQLException e) {
                trace.error(e, "Rollback {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }

    };

    private class SavePoint implements Callback<Savepoint> {
        private final String savepaintName;

        public SavePoint(String name) {
            this.savepaintName = name;
        }

        @Override
        public Savepoint handle(String name, Connection connection) throws DbException {
            try {
                Savepoint setSavepoint;
                if (!StringUtils.isNullOrEmpty(savepaintName)) {
                    setSavepoint = connection.setSavepoint(savepaintName);
                } else {
                    setSavepoint = connection.setSavepoint();

                }
                return setSavepoint;
            } catch (SQLException e) {
                trace.error(e, "setSavepoint {0} connection error", name);
                throw DbException.convert(e);
            }

        }
    };

    private class Closer implements Callback<String> {
        @Override
        public String handle(String name, Connection connection) throws DbException {
            try {
                target.closeConnection(connection, Options.build().shardName(name));
            } catch (Exception e) {
                trace.error(e, "Close {0} connection error", name);
                throw DbException.convert(e);
            }
            return name;
        }

    };

}
