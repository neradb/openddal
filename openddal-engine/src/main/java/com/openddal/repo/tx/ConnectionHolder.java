package com.openddal.repo.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import com.openddal.util.New;

public class ConnectionHolder {


    private Map<String, ConnectionStatus> connectionMap = New.hashMap();

    public synchronized Connection getConnection(String shardName, DataSource ds) throws SQLException {
        ConnectionStatus status = connectionMap.get(shardName);
        if (status == null) {
            status = new ConnectionStatus();
            status.connection = ds.getConnection();
            connectionMap.put(shardName, status);
        }
        if (status.isRelease) {
            status.isRelease = false;
            return status.connection;
        }
        throw new SQLException("The transaction connection is occupied by other thread");
    }

    public synchronized boolean release(String shardName) throws SQLException {
        ConnectionStatus status = connectionMap.get(shardName);
        if (status != null) {
            status.isRelease = true;
            return true;
        }
        return false;
    }

    public synchronized <T> List<T> foreach(ConnectionCallback<T> callback) throws SQLException {
        List<T> results = New.arrayList();
        for (String name : connectionMap.keySet()) {
            ConnectionStatus status = connectionMap.get(name);
            results.add(callback.handle(name, status.connection));
        }
        return results;
    }

    public synchronized <T> List<T> foreach(Set<String> shards, ConnectionCallback<T> callback) throws SQLException {
        List<T> results = New.arrayList();
        for (String name : connectionMap.keySet()) {
            if (shards.contains(name)) {
                ConnectionStatus status = connectionMap.get(name);
                results.add(callback.handle(name, status.connection));
            }
        }
        return results;
    }

    public static interface ConnectionCallback<T> {
        T handle(String name, Connection connection) throws SQLException;
    }
    

    private class ConnectionStatus {
        private Connection connection;
        private boolean isRelease = true;
    }

}
