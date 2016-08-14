
package com.openddal.server.core;

public interface QueryResult {

    public int getType();

    public void close();

    short getWarnings();

    String getMessage();

    int getRows();
}
