
package com.openddal.server.core;

import com.openddal.result.LocalResult;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class QueryResult implements AutoCloseable {

    public static final int UPDATE_RESULT = 1;
    public static final int SELECT_RESULT = 2;

    private final int type;
    private int affectedRows;
    private LocalResult result;

    private QueryResult(int affectedRows) {
        this.type = UPDATE_RESULT;
        this.affectedRows = affectedRows;
    }

    private QueryResult(LocalResult result) {
        this.type = SELECT_RESULT;
        this.result = result;
    }

    public int getType() {
        return type;
    }

    public int getUpdateResult() {
        return affectedRows;
    }

    public LocalResult getSelectResult() {
        return result;
    }

    @Override
    public void close() throws Exception {
        if (result != null) {
            result.close();
        }
    }
}
