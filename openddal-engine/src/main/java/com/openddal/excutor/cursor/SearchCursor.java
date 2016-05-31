/*
 * Copyright 2014-2016 the original author or authors
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
package com.openddal.excutor.cursor;

import java.util.ArrayList;
import java.util.HashSet;

import com.openddal.command.dml.Select;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SearchCursor implements Cursor {


    private Session session;
    private final Select select;
    private final TableFilter topFilters;
    private boolean alwaysFalse;

    private Cursor cursor;

    public SearchCursor(TableFilter topFilters) {
        this.topFilters = topFilters;
        this.select = topFilters.getSelect();
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param s Session.
     * @param indexConditions Index conditions.
     */
    public void prepare(Session s, ArrayList<IndexCondition> indexConditions) {
        this.session = s;
        alwaysFalse = false;
    
        // don't use enhanced for loop to avoid creating objects
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        prepare(s, indexConditions);
        if (!alwaysFalse) {
            
        }
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
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
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {
        
    }


    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }


}
