package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.openddal.engine.spi.Transaction;
import com.openddal.message.Trace;
import com.openddal.repo.JdbcRepository;
import com.openddal.repo.tx.ConnectionHolder.ConnectionCallback;

public class JdbcTransaction implements Transaction {
    
    private ConnectionHolder connectionHolder;
    private Trace trace;
    private boolean isEnd;
    
    
    public JdbcTransaction(JdbcRepository repo) {
        this.trace = repo.getDatabase().getTrace(Trace.TRANSACTION);
        this.connectionHolder = new ConnectionHolder();
    }
    
    

    public ConnectionHolder getConnectionHolder() throws SQLException {
        checkClosed();
        return connectionHolder;
    }


    @Override
    public void commit() {
        checkClosed();
        List<String> foreach = connectionHolder.foreach(new Commiter());
        if(trace.isDebugEnabled()) {
            trace.debug("commit connections {0}", foreach);
        }
        close();
    }

    @Override
    public void rollback() {
        try {
            checkClosed();
            List<String> foreach = connectionHolder.foreach(new Rollbacker());
            if(trace.isDebugEnabled()) {
                trace.debug("rollback connections {0}", foreach);
            }
        } catch (SQLException e) {
            //throw DbException.get(ErrorCode.)
        }
        close();
    }

    @Override
    public void close() {
        try {
            connectionHolder.foreach(new Closer());
            connectionHolder = null;
            trace = null;
        } finally {
            isEnd = true;
        }
    }
    
    
    public void checkClosed() throws SQLException {
        if(isEnd) {
            throw new SQLException("transaction ended.");
        }
    }
    
    
   private class Commiter implements ConnectionCallback<String> {
        @Override
        public String handle(String name, Connection connection) throws SQLException {
            try {
                connection.commit();
            } catch (SQLException e) {
                trace.error(e, "Commit {0} connection error", name);
                throw e;
            }
            return name;
        }
        
    };
    
    
    private class Rollbacker implements ConnectionCallback<String> {
        @Override
        public String handle(String name, Connection connection) throws SQLException {
            try {
                connection.rollback();
            } catch (SQLException e) {
                trace.error(e, "Rollback {0} connection error", name);
                throw e;
            }
            return null;
        }
        
    };
    
    
    private class Closer implements ConnectionCallback<String> {
        @Override
        public String handle(String name, Connection connection) throws SQLException {
            try {
                connection.close();
            } catch (Exception e) {
                trace.error(e, "Close {0} connection error", name);
            }
            return name;
        }
        
    };

}
