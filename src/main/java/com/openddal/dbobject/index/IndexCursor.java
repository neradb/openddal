/*
 * Copyright 2014-2015 the original author or authors
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
package com.openddal.dbobject.index;

import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.IndexColumn;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;

import java.util.ArrayList;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class IndexCursor implements Cursor {

    private final TableFilter tableFilter;
    private Session session;
    private Index index;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;
    private SearchRow start, end;
    private Cursor cursor;

    public IndexCursor(TableFilter filter) {
        this.tableFilter = filter;
    }

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s               the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        Expression filter = tableFilter.getFilterCondition();
        if (filter == null) {

        }
    }

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        if (cursor == null) {
            return false;
        }
        return cursor.next();
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

}
