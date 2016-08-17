
package com.openddal.server.core;

import com.openddal.result.ResultInterface;

/**
 * @author jorgie.li
 */
public class QueryResult implements AutoCloseable {

    private static final int UPDATE_RESULT = 1;
    private static final int SELECT_RESULT = 2;

    private final int type;
    private int affectedRows;
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

    @Override
    public void close() throws Exception {
        if (result != null) {
            result.close();
        }
    }
}
