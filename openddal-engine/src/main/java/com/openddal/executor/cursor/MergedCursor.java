package com.openddal.executor.cursor;

import java.util.List;

import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.util.New;

public class MergedCursor implements Cursor {

    private Cursor cursor;
    private int index;
    private List<Cursor> cursors = New.arrayList(10);
    
    public void addCursor(Cursor cursor) {
        cursors.add(cursor);
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
        cursor = index < cursors.size() ? cursors.get(index) : null;
        ++index;
    }
    @Override
    public boolean previous() {
        return false;
    }

}
