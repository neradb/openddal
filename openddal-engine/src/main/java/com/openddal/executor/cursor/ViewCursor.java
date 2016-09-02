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
package com.openddal.executor.cursor;

import com.openddal.dbobject.table.TableView;
import com.openddal.message.DbException;
import com.openddal.result.LocalResult;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;

/**
 * The cursor implementation of a view index.
 */
public class ViewCursor implements Cursor {

    private final LocalResult result;
    private Row current;
    private TableView view;

    public ViewCursor(TableView view, LocalResult result) {
        this.view = view;
        this.result = result;
    }

    @Override
    public Row get() {
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return current;
    }

    @Override
    public boolean next() {
        while (true) {
            boolean res = result.next();
            if (!res) {
                if (view.isRecursive()) {
                    result.reset();
                } else {
                    result.close();
                }
                current = null;
                return false;
            }
            current = view.getTemplateRow();
            Value[] values = result.currentRow();
            for (int i = 0, len = current.getColumnCount(); i < len; i++) {
                Value v = i < values.length ? values[i] : ValueNull.INSTANCE;
                current.setValue(i, v);
            }
            return true;
        }
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
