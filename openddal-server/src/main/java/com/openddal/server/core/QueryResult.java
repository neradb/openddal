
package com.openddal.server.core;

import com.openddal.result.ResultInterface;

/**
 * @author jorgie.li
 */
public class QueryResult {

    private static final int UPDATE_RESULT = 1;
    private static final int SELECT_RESULT = 2;

    private final int type;
    private int affectedRows;
    private short warnings;
    private String message;
    private long insertId;
    private ResultInterface result;

    public QueryResult(int affectedRows) {
        this.type = UPDATE_RESULT;
        this.affectedRows = affectedRows;
    }

    public QueryResult(ResultInterface result) {
        this.type = SELECT_RESULT;
        this.result = result;
    }

    public boolean isQuery() {
        return type == SELECT_RESULT;
    }

    public int getUpdateResult() {
        return affectedRows;
    }

    public ResultInterface getQueryResult() {
        return result;
    }

    public short getWarnings() {
        return warnings;
    }

    public void setWarnings(short warnings) {
        this.warnings = warnings;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getInsertId() {
        return insertId;
    }

    public void setInsertId(long insertId) {
        this.insertId = insertId;
    }

    public void close() {
        if (result != null) {
            result.close();
        }
    }
}
